from datetime import datetime, timedelta

from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from dotenv import load_dotenv
from airflow.models import Variable

import sys
import logging
import os
import requests
import rdflib
import ssl
import json

log = logging.getLogger("airflow.task.operators")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)

default_args = {
    'owner' : 'red',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

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

def add_csvw(token: str, pmd_api_endpoint: str, draft_id: str, metadata_file):
    """Upload a CSVW to a draft in PMD."""

    #metadata = metadata_file.text
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

def authenticate():
    logger = logging.getLogger("airflow.task")

    load_dotenv()
    token = pmd_authenticate(
        Variable.get("PMD_AUTH0_ENDPOINT"),
        Variable.get("PMD_CLIENT_ID"),
        Variable.get("PMD_CLIENT_SECRET"),
    )
    logger.info("Creating draft")
    draft_id = pmd_create_draft(token=token, pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"))
    logger.info(f"Draft ID: {draft_id}")

    return token, draft_id

def create_draft(**kwargs):
    ti = kwargs['ti']
    params = ti.xcom_pull(task_ids='authenticate')
    token = params[0]
    draft_id = params[1]

    logger = logging.getLogger("airflow.task")

    load_dotenv()
    logger.info("Creating draft")
    
    logger.info(f"Draft ID: {draft_id}")
    logger.info("Adding CSVW metadata to draft")
    add_csvw(
        token=token,
        pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"),
        draft_id=draft_id,
        metadata_file= "example-files/out/4g-coverage.csv-metadata.json"
    )
    logger.info("Adding PMDCAT metadata to draft")
    add_pmdcat(
        token=token,
        pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"),
        draft_id=draft_id,
        metadata_file= "example-files/out/4g-coverage.csv-metadata.json"
    )
    # Need to the run csv2rdf step manually for now
    # docker run --rm -v $PWD:/workspace -w /workspace -it gsscogs/csv2rdf \
    # csv2rdf -u life-expectancy-by-region-sex-and-time.csv-metadata.json -m minimal -o output.ttl
    logger.info("Adding observations to draft")
    add_observations(
        token=token,
        pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"),
        draft_id=draft_id,
        metadata_file="example-files/out/4g-coverage.csv-metadata.json",
        observations_file="example-files/out/output.ttl",
    )

def test_tasks():
    print("PMD_AUTH0_ENDPOINT: " + Variable.get("PMD_AUTH0_ENDPOINT")),
    print("PMD_CLIENT_ID: " + Variable.get("PMD_CLIENT_ID")),
    print("PMD_CLIENT_SECRET: " + Variable.get("PMD_CLIENT_SECRET"))


with DAG(
    default_args = default_args,
    dag_id = 'Auth_and_push_to_drafter',
    description = 'Authenticate user to staging and create draft',
    start_date = datetime(2023, 5, 22),
    schedule_interval = '@daily'
) as dag:
    #task0 = DockerOperator(
    #    task_id = 'csvlint',
    #    image = 'gsscogs/csvlint',
    #    command = "csvlint -s https://raw.githubusercontent.com/RedWalters/prototype_pipeline/main/example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json",
    #    docker_url = 'tcp://docker-proxy:2375',
    #    network_mode = 'host',
    #    working_dir = '/opt/airflow/example-files/out'
    #)    
    task1 = BashOperator(
        task_id = 'csvcubed',
        bash_command = 'csvcubed build ${AIRFLOW_HOME}/example-files/4g-coverage.csv',
        cwd='example-files'
    )    
    task2= PythonOperator(
        task_id = 'authenticate',
        python_callable = authenticate
    )
    task3 = PythonOperator(
        task_id = 'create_draft',
        python_callable = create_draft,
        provide_context = True
    )   
    
    task1 >> task2 >> task3


    #Currently the below will run successfully (with the corresponding stuff added to docker-compose)
    #but I havent figured out how to return the output.ttl yet so it doesnt actually return anything at the moment
    
    #task4 = DockerOperator(
    #    task_id = 'csv2rdf',
    #    image = 'gsscogs/csv2rdf',
    #    command = "csv2rdf -u https://raw.githubusercontent.com/GSS-Cogs/poc-pipelines/main/example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json -m minimal -o output.ttl",
    #    docker_url = 'tcp://docker-proxy:2375',
    #    network_mode = 'bridge',
    #    working_dir = '/opt/airflow/example-files'
    #)    

    #task3 = DockerOperator(
    #    task_id = 'csvlint',
    #    image = 'gsscogs/csvlint',
    #    command = "csvlint -s example-files/4g-coverage.csv",
    #    docker_url = 'tcp://docker-proxy:2375',
    #    network_mode = 'host',
    #    mount_tmp_dir = False,
    #    xcom_all=True
    #)

    #task4 >> task1 >> task2
  
    #task3 = DockerOperator(
    #    task_id = 'csvlint',
    #    image = 'gsscogs/csvlint',
    #    command = "csvlint -s :/opt/airflow/example-files/4g-coverage.csv",
    #    docker_url = 'tcp://docker-proxy:2375',
    #    network_mode = 'host',
    #    working_dir = '/opt/airflow/example-files/out'
    #)

    #task3