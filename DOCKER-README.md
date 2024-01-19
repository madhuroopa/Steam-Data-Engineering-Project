# Setting Up Docker 🐳
Let's set up Docker with Apache Airflow and PySpark.

## Steps 
- Download Docker for Desktop from [Official Docker Website](https://www.docker.com/products/docker-desktop/)
- Docker files are in /docker folder.
- Download the custom `docker-compose.yaml` file from `docker/`
- Setup `.env` file from `docker/`
- Create folders: dags, logs, config, plugins, code. These folders are used by docker. 📁
- Put your DAG .py code in [`/dags`](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/tree/main/docker/sample)
- Navigate to your project that contains the .yaml file and run `docker-compose up`
- Airflow should start at: `http://localhost:8080`
- The username and password is set to: `airflow` 

## Info ℹ
```
volumes:
#Add additional volumes to mount code and data
- ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
- ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
- ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
- ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
- ${AIRFLOW_PROJ_DIR:-.}/code:/opt/airflow/code
```
- The above part of code, moves 🚚 the files from local /dags, /logs, /config, /plugins, /code to their respective paths in docker container.
- If you want to add additional Python libraries mention them after `_PIP_ADDITIONAL_REQUIREMENTS:` in the .yaml file.
- Check `docker/sample` for folder structure and boilerplate DAG code. 

## Note 📝
- I do not own the code in the `docker-compose.yaml` file. However I have used it under Apache license and modified it for my purpose.
- `/docker/sample/logs/` have been zipped 
