# poc-pipelines

To run the project we set up a virtual environment and install the dependencies.

```sh
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
```

The [`main.py`](main.py) script is the entry point for the project.

It expects a `.env` file to be present in the root of the project. This file should contain the following environment variables:

```sh
PMD_AUTH0_ENDPOINT=""
PMD_API_ENDPONT=""
PMD_CLIENT_ID=""
PMD_CLIENT_SECRET=""
```

The [`docker-compose.yml`](docker-compose.yaml) is taken from the [airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

To run airflow for the first time we must initialise an admin account

```sh
docker compose up airflow-init
```

After this, we can start airflow by starting all the containers.

```sh 
docker compose up
```

## Docs

- [Dissemination application profile](https://github.com/GSS-Cogs/application-profile)
- [PMD swagger specification](https://idp-beta-drafter.publishmydata.com/index.html)
- [PMD API documentation](https://swirrl.github.io/PMD-AP/index.html)

## Steps

An example [CSV file](example-files/life-expectancy-by-region-sex-and-time.csv) and [CSVW metadata file](example-files/life-expectancy-by-region-sex-and-time.csv-metadata.json) can be found in the [`example-files`](example-files) directory.

Given a CSV file and CSVW metadata file which describe a statistical dataset:
- Get the `dcat:Dataset`'s IRI.
- Construct a `dcat:CatalogRecord` IRI as `{DATASET_IRI}/record`.
- Add all triples from the CSVW metadata file to a named graph using the `dcat:CatalogRecord` IRI.
- Explicitly insert triples adding classes for the `csvw:Table`, `csvw:TableSchema` and `csvw:Column`s.
- Construct `PMDCAT` metadata and add it to a named graph using the `dcat:CatalogRecord` IRI.
- Run `csv2rdf` to convert the CSV file to RDF, producing RDF observations.
- Get the `qb:DataSet`'s IRI.
- Add all observation triples to a named graph using the `qb:DataSet` IRI.


## To do

- Craft the functions in `main.py` into an airflow [taskflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html) pipeline, following [best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).
- Similar approach for concept schemes to datasets:
    - We have to construct `skos:narrower` relationships between concepts.
    - We have to construct `skos:hasTopConcept` relationships between the concept scheme and its top concepts.
- Running PMD validation tests and RDF data cube integrity constraints.
