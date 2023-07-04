
## Running this on your local machine

For this to run correctly you will have to update the docker-compose and the main.py to match your local folder layout as it is currently hardcoded to point to certain locations on my machine
I need to go through and replace these with local folder directions but I haven't yet

If you encouter import issues when on the Airflow dashboard you may need to run 'docker-compose build' to update the docker container with the requirements file.

## Launching the Airflow Webserver

Assuming you have none of the issues above:

In the root prototype_pipeline folder run 'docker-compose up airflow-init' to run the Airflow docker container.

Then run 'docker-compose up -d' to launch the webserver. 

Then navigate to 'localhost:8080/' on your web browser of choice and login with the default airflow credentials 'airflow' 'airflow'.

You should then just be able to run the 'Auth_and_push_to_drafter' DAG from there. 

### Any issues

There is probably something I'm forgetting or an issue I solved and forgot to add.
Anything like this let me know and I'll update this.

