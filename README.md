# IS3107 Project

## Deployed Application

View our app here: [JobMiner Insider](https://tsjgetxoyukulwgq7m8hhv.streamlit.app/)
View the repository for the app here: [JobMiner Insider GitHub](https://github.com/zuohui48/streamlit)

## Setting up

Create a python3.10 virtual environment and activate it.

Initialise your environment with the necessary libraries

```bash
pip install -r requirements.txt
```

Run these lines in the terminal to configure your airflow environment and build the image

```bash
echo -e "AIRFLOW_UID=$(id -u) > .env"

docker compose build
docker compose up
```

## Dependency Management

Whenever you add a new package to your project, make sure to run

```bash
pip freeze > requirements.txt
```
