# oncokb-variant-recommendation

## Running Locally

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

## Running Tests

```sh
pytest
```

- Note that all test files must end in `_test.py`

## Create/Update requirements file

```sh
pip freeze > requirements.txt
```
