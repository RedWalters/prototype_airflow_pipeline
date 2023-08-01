from datetime import datetime, timedelta

from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv
from airflow.models import Variable
from docker.types import Mount

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
    'retries' : 1,
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

def create_draft(*args, **kwargs,):
    file_name = args[0]
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
        metadata_file= "example-files/out/" + file_name #4g-coverage.csv-metadata.json"
    )
    logger.info("Adding PMDCAT metadata to draft")
    add_pmdcat(
        token=token,
        pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"),
        draft_id=draft_id,
        metadata_file= "example-files/out/" + file_name #4g-coverage.csv-metadata.json"
    )
    logger.info("Adding observations to draft")
    add_observations(
        token=token,
        pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT"),
        draft_id=draft_id,
        metadata_file="example-files/out/" + file_name, #4g-coverage.csv-metadata.json"
        observations_file="example-files/out/" + file_name.split('.')[0] + ".ttl" #4g-coverage.ttl",
    )

    return draft_id

def submitTo(**kwargs):
    ti = kwargs['ti']
    params = ti.xcom_pull(task_ids='authenticate')
    token = params[0]
    draft_id = params[1]
    pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT")
    """Create a draft in PMD and return the draft ID."""
    print(pmd_api_endpoint)
    req = requests.post(
        f"{pmd_api_endpoint}/draftset/{draft_id}/submit-to",
        headers={"Authorization": f"Bearer {token}", "Content-Type" : "application/json"},
        json={"role" : "editor"},
    )
    req.raise_for_status()
    return req.json().get("id")

#def claim_draft(**kwargs):
#    ti = kwargs['ti']
#    params = ti.xcom_pull(task_ids='authenticate')
#    token = params[0]
#    draft_id = params[1]
#    pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT")
#    """Create a draft in PMD and return the draft ID."""
#    print(pmd_api_endpoint)
#    req = requests.put(
#        f"{pmd_api_endpoint}/draftset/{draft_id}/claim",
#        headers={"Authorization": f"Bearer {token}"}
#    )
#    req.raise_for_status()
#    return req.json().get("id")

def deleteDraft(**kwargs):
    ti = kwargs['ti']
    params = ti.xcom_pull(task_ids='authenticate')
    token = params[0]
    draft_id = params[1]
    pmd_api_endpoint=Variable.get("PMD_API_ENDPOINT")
    req = requests.delete(
        f"{pmd_api_endpoint}/draftset/{draft_id}",
        headers={"Authorization": f"Bearer {token}"}
    )
    req.raise_for_status()
    return req.json().get("id")


def test_tasks(*args, **kwargs):
    print(args)
    print(kwargs)
    ti = kwargs['ti']
    params = ti.xcom_pull(task_ids='authenticate')
    token = params[0]
    draft_id = params[1]
    return draft_id

def multiple_csv_error():
    raise ValueError('Multiple Input CSV files detected, please ensure only 1 source Tidy CSV is present.')


path_to_json = './example-files/out/' 
metadata_json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('metadata.json')]

path_to_source = './example-files/'
input_file = [file for file in os.listdir(path_to_source) if file.endswith('.csv')]
#EmptyOperator.ui_color = '#008000'

with DAG(
    default_args = default_args,
    dag_id = 'Auth_and_push_to_drafter_v1',
    description = 'Authenticate user to staging and create draft',
    start_date = datetime(2023, 7, 30),
    schedule_interval = '@daily'
) as dag:
    if len(input_file) == 1:
        tidy_csv = input_file[0]
        #[x for x in metadata_json_files if not (tidy_csv.split('.')[0] in x)]

        
        auth= PythonOperator(
            task_id = 'authenticate',
            python_callable = authenticate
        )
        #csvcubed = BashOperator(
        #    task_id = 'csvcubed',
        #    #bash_command = 'csvcubed build ${AIRFLOW_HOME}/example-files/4g-coverage.csv',
        #    bash_command = 'csvcubed build ${AIRFLOW_HOME}/example-files/' + tidy_csv,
        #    cwd='example-files'
        #)  
        #createDraft = PythonOperator(
        #    task_id = 'create_draft',
        #    python_callable = create_draft, 
        #    provide_context = True
        #)  
        #test = PythonOperator(
        #    task_id = 'sbkndfklnsb',
        #    python_callable = test_tasks,
        #    op_args=["test"]
        #) 
        passToUser = PythonOperator(
            task_id = 'passToUser',
            python_callable = submitTo
        )
        #claimDraft = PythonOperator(
        #    task_id = 'claimDraft',
        #    python_callable = claim_draft
        #)
        #removeDraft = PythonOperator(
        #    task_id = 'removeDraft',
        #    python_callable = deleteDraft
        #) 

        #csvlint_source = DockerOperator(
        #        task_id = 'csvlint_{}'.format(tidy_csv),
        #        image = 'gsscogs/csvlint',
        #        command = "csvlint -s /example-files/out/" + tidy_csv + "-metadata.json",
        #        mounts=[Mount(source="c:/Users/Red/Documents/COGS/Airflow/poc-pipelines-main/example-files", target="/example-files", type="bind")],
        #        docker_url='tcp://docker-proxy:2375',
        #        network_mode='bridge',
        #        mount_tmp_dir=False
        #    )
        #csv2rdf_source = DockerOperator(
        #        task_id = 'csv2rdf_{}'.format(tidy_csv),
        #        image = 'gsscogs/csv2rdf',
        #        command = "csv2rdf -u /example-files/out/{} -m minimal -o /example-files/out/{}.ttl".format(tidy_csv + "-metadata.json", tidy_csv.split('.')[0]) ,
        #        mounts=[Mount(source="c:/Users/Red/Documents/COGS/Airflow/poc-pipelines-main/example-files", target="/example-files", type="bind")],
        #        docker_url='tcp://docker-proxy:2375',
        #        network_mode='bridge',
        #        mount_tmp_dir=False
        #    )
        create_draft_source = PythonOperator(
                task_id = 'create_draft_{}'.format(tidy_csv + "-metadata.json",),
                python_callable = create_draft,
                op_args = [tidy_csv + "-metadata.json"],
                provide_context = True
            )
        
        auth >> create_draft_source >> passToUser

        #auth >> csvcubed >> csvlint_source >> csv2rdf_source >> create_draft_source >> passToUser
        
        #for file in metadata_json_files:
        #    if tidy_csv not in file:
        #        csvlint = DockerOperator(
        #                task_id = 'csvlint_{}'.format(file),
        #                image = 'gsscogs/csvlint',
        #                command = "csvlint -s /example-files/out/{}".format(file),
        #                mounts=[Mount(source="c:/Users/Red/Documents/COGS/Airflow/poc-pipelines-main/example-files", target="/example-files", type="bind")],
        #                docker_url='tcp://docker-proxy:2375',
        #                network_mode='bridge',
        #                mount_tmp_dir=False
        #            )
        #        csv2rdf = DockerOperator(
        #                task_id = 'csv2rdf_{}'.format(file),
        #                image = 'gsscogs/csv2rdf',
        #                command = "csv2rdf -u /example-files/out/{} -m minimal -o /example-files/out/{}.ttl".format(file, file.split('.')[0]) ,
        #                mounts=[Mount(source="c:/Users/Red/Documents/COGS/Airflow/poc-pipelines-main/example-files", target="/example-files", type="bind")],
        #                docker_url='tcp://docker-proxy:2375',
        #                network_mode='bridge',
        #                mount_tmp_dir=False
        #            )
        #        create_drafts = PythonOperator(
        #                task_id = 'create_draft_{}'.format(file),
        #                python_callable = create_draft,
        #                op_args = [file],
        #                provide_context = True
        #            )
        #    
        #        passToUser >> csvlint >> csv2rdf >> create_drafts
    
    else:
        removeDraft = PythonOperator(
            task_id = 'Error',
            python_callable = multiple_csv_error
            
        )  

        