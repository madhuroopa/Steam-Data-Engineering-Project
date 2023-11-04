##Steps to run

- Clone the repo
- `cd airflow`
- `docker compose up airflow-init`
- `docker build . --tag extending_airflow:latest`
- `docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler`
- Go to `http://localhost:8080/`  : username & password : `airflow`
- Unpause the DAG `dag_with_python_dependencies_v03` and Click play
- Check logs
- To stop the airflow server : `docker-compose down`
- To re-start the dashboard, after compose down `docker-compose up -d airflow-webserver airflow-scheduler`


# Credits
- https://github.com/coder2j/airflow-docker/tree/main