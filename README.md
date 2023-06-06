# prototype_airflow_pipeline

Currently the pipeline only runs csvcubed to a local folder, checks the users PMD auth variables, and then creates a draft on staging.

This won't work straight out of the box for anyone as the local folder paths in the docker-compose.yaml are set up for my local machine.
Also you will have to run 'docker-compose build' to ensure the requirements are installed to the docker container from the Dockerfile/Requirements.txt

The auth variables are currently coded to be taken from the Airflow variabe manager so you'll have to either upload yours into there when you launch the webserver, or hardcode them into the script. 

Lastly, csv2rdf is currently manually ran and the ttl file left in the relevant folder as I have yet to figure out to get the dockerOperator to output :@
