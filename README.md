# oncokb-variant-recommendation

## Set up python environment
### Running Locally

1. First Time Setup

   1. Create a python [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
      the .venv folder for python to use

      ```sh
      python3 -m venv .venv
      ```

   2. Create a .env

      ```sh
      cp env.example .env
      ```

2. Activating Virtual Environment

   1. Make python download and use open source code inside your .venv folder

      ```sh
      source .venv/bin/activate
      ```

   2. Check if python using .venv folder

      ```sh
      which python3
      ```

3. Install the latest packages for the project

   ```sh
   pip install -r requirements.txt
   ```

### Running Tests

```sh
pytest
```

- Note that all test files must end in `_test.py`


### Create/Update requirements file

```sh
pip freeze > requirements.txt
```

## Set Airflow
### Install Airflow 
```sh
pip install 'apache-airflow==2.9.1' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.8.txt"
```
- Change constraints-3.8.txt to your python version
- More details in [Airflow document](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi)

### Initialize Airflow
1. Create an airflow folder under your object, and create a dags folder under airflow. 

2. To configure Airflow to recognize your DAGs directory, you need to set the `AIRFLOW_HOME` environment variable. replacing `/path/to/dags/folder/parent/folder` with the actual path to your desired directory:
   
```sh
export AIRFLOW_HOME= path/to/dags/flod/parent/flod
```

3. Run Airflow Standalone, and get the default username and password.

```sh
airflow standalone
```

The command initializes the database, creates a user, and starts all components.

- If you prefer to run individual components of Airflow manually, or if you need personalized user information, instead of using the all-in-one standalone command, you can run the following:
```sh
airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

airflow webserver --port 8080

airflow scheduler

airflow triggerer
```
- More details in [Airflow document](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

4. Access the Airflow UI:
Visit localhost:8080 in your browser.

5. Connect to AWS S3: Choose connections under Admin, create a new connection. Input `oncokb_s3` in `Connection Id`, choose `Connection Type` as Amazon Web Services , and input `AWS Access Key ID` and `AWS Secret Access Key`.

6. Connect to MySQL: Choose connections under Admin, create a new connection. Input `oncokb_mysql` in `Connection Id`, choose `Connection Type` as MySQL , and input`Host`,`schema`,`login`,`password` and `port`.

### Note:
- If you want to close all Airflow DAG or connectionsexamples on Airflow webserver. Open airflow.cfg and change `load_examples = False` or `load_default_connections = False`.

## Test Airflow
With the Airflow CLI, run to test your dag, you can check the result and logs at Airflow UI.
```sh
airflow dags test <dag_id>
```

You can use CLI to list all dags you have.
```
airflow dags list
```

