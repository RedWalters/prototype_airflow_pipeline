from airflow.decorators import dag, task
from datetime import datetime
from dotenv import load_dotenv

import logging
import os
import requests
import rdflib
import ssl

# Needed to avoid SSL errors when using requests
ssl._create_default_https_context = ssl._create_unverified_context


def get_dataset_iri(graph: rdflib.Graph) -> str:
    """Get the dataset IRI from an rdflib graph. This is used to help construct
    the IRI of the named graph in which the metadata is stored."""
    dataset = graph.query(
        """
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        SELECT ?ds WHERE {
            ?ds a dcat:Dataset ;
        }
        LIMIT 1
        """
    )
    for row in dataset:
        dataset = row.get("ds")
    return dataset


def get_datacube_iri(graph: rdflib.Graph) -> str:
    """Get the datacube IRI from an rdflib graph. This is used to help construct
    the IRI of the named graph in which the observations are stored."""
    cube = graph.query(
        """
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT ?qb WHERE {
            ?qb a qb:DataSet ;
        }
        LIMIT 1
        """
    )
    for row in cube:
        cube = row.get("qb")
    return cube


# @task
def pmd_authenticate(
    pmd_auth0_endpoint: str,
    pmd_client_id: str,
    pmd_client_secret: str,
):
    """Authenticate with PMD and return a token."""
    req = requests.post(
        pmd_auth0_endpoint,
        json={
            "client_id": pmd_client_id,
            "client_secret": pmd_client_secret,
            "audience": "https://pmd",
            "grant_type": "client_credentials",
        },
    )
    req.raise_for_status()
    token = req.json().get("access_token")
    return token


# @task
def pmd_create_draft(
    token: str,
    pmd_api_endpoint: str,
    display_name: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    description: str = None,
):
    """Create a draft in PMD and return the draft ID."""
    req = requests.post(
        f"{pmd_api_endpoint}/draftsets",
        headers={"Authorization": f"Bearer {token}"},
        json={"display-name": display_name, "description": description},
    )
    req.raise_for_status()
    return req.json().get("id")


# @task
def add_csvw(token: str, pmd_api_endpoint: str, draft_id: str, metadata_file: str):
    """Upload a CSVW to a draft in PMD."""
    graph = rdflib.Graph()
    graph.parse(metadata_file)

    # The metadata from the CSVW is stored in a named graph which is given an
    # IRI which is the same as the catalogue record's IRI.
    graph_name = f"{get_dataset_iri(graph)}/record"

    # We insert CSVW types which we don't make explicit in the CSVW
    graph.update(
        """
        PREFIX csvw: <http://www.w3.org/ns/csvw#>
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        INSERT {
            ?table a csvw:Table .
            ?tableSchema a csvw:TableSchema .
            ?column a csvw:Column .
        }
        WHERE {
            ?ds a dcat:Dataset ;
                dcat:distribution ?table .

            ?table csvw:tableSchema ?tableSchema .
            ?tableSchema csvw:column/rdf:rest*/rdf:first ?column .
            
        }
        """
    )

    # PMD doesn't accept JSON-LD, so we need to convert it to Turtle
    csvw = graph.serialize(format="turtle", encoding="utf-8").decode("utf-8")
    req = requests.put(
        f"{pmd_api_endpoint}/draftset/{draft_id}/data",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/turtle",
        },
        data=csvw,
        params={"graph": graph_name},
    )
    return req.json()


# @task
def add_pmdcat(token: str, pmd_api_endpoint: str, draft_id: str, metadata_file: str):
    """Add PMDCAT metadata to a draft in PMD."""
    graph = rdflib.Graph()
    graph.parse(metadata_file)

    # The metadata from the CSVW is stored in a named graph which is given an
    # IRI which is the same as the catalogue record's IRI.
    graph_name = f"{get_dataset_iri(graph)}/record"

    # Construct PMDCAT types and relations.
    res = graph.query(
        """
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX pmdcat: <http://publishmydata.com/pmdcat#>
        PREFIX qb: <http://purl.org/linked-data/cube#>
        CONSTRUCT {
            <http://gss-data.org.uk/catalog/datasets> dcat:record ?record .

            ?record a dcat:CatalogRecord ;
                foaf:primaryTopic  ?ds ;
                pmdcat:metadataGraph ?record .

            ?ds a pmdcat:Dataset ;
                pmdcat:datasetContents ?qb ;
                pmdcat:graph ?qb .

            ?qb a pmdcat:DataCube .
        }
        WHERE {
            ?ds a dcat:Dataset .
            ?qb a qb:DataSet .
            BIND(IRI(CONCAT(STR(?ds), "/record")) AS ?record)
        }
        """
    )

    pmdcat = res.serialize(format="turtle", encoding="utf-8").decode("utf-8")
    req = requests.put(
        f"{pmd_api_endpoint}/draftset/{draft_id}/data",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/turtle",
        },
        data=pmdcat,
        params={"graph": graph_name},
    )
    return req.json()


# @task
def add_observations(
    token: str,
    pmd_api_endpoint: str,
    draft_id: str,
    metadata_file: str,
    observations_file: str,
):
    """Add observations to a draft in PMD."""
    graph = rdflib.Graph()
    graph.parse(metadata_file)
    # The dataset observations are stored in a named graph which is given an
    # IRI which is the same as the qb:DataSet's IRI.
    graph_name = get_datacube_iri(graph)
    req = requests.put(
        f"{pmd_api_endpoint}/draftset/{draft_id}/data",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/turtle",
        },
        data=open(observations_file, "rb"),
        params={"graph": graph_name},
    )
    return req.json()


logger = logging.getLogger("airflow.task")

load_dotenv()
logger.info("Authenticating with PMD")
token = pmd_authenticate(
    os.getenv("PMD_AUTH0_ENDPOINT"),
    os.getenv("PMD_CLIENT_ID"),
    os.getenv("PMD_CLIENT_SECRET"),
)
logger.info("Creating draft")
draft_id = pmd_create_draft(token=token, pmd_api_endpoint=os.getenv("PMD_API_ENDPONT"))
logger.info(f"Draft ID: {draft_id}")
logger.info("Adding CSVW metadata to draft")
add_csvw(
    token=token,
    pmd_api_endpoint=os.getenv("PMD_API_ENDPONT"),
    draft_id=draft_id,
    metadata_file="example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json",
)
logger.info("Adding PMDCAT metadata to draft")
add_pmdcat(
    token=token,
    pmd_api_endpoint=os.getenv("PMD_API_ENDPONT"),
    draft_id=draft_id,
    metadata_file="example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json",
)
# Need to the run csv2rdf step manually for now
# docker run --rm -v $PWD:/workspace -w /workspace -it gsscogs/csv2rdf \
# csv2rdf -u life-expectancy-by-region-sex-and-time.csv-metadata.json -m minimal -o output.ttl
logger.info("Adding observations to draft")
add_observations(
    token=token,
    pmd_api_endpoint=os.getenv("PMD_API_ENDPONT"),
    draft_id=draft_id,
    metadata_file="example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json",
    observations_file="example-files/life-expectancy-by-region-sex-and-time.ttl",
)
