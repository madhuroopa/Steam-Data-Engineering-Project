## Running Without AWS.
- aka. Running Kafa, Spark and Airflow independently

# Running Kafka independently
- Uncomment the `if __name__ == "__main__":` from each *_kafka_consumer and *_kafka_producer.
- Run kafka zookeeper and kafka server in two seprate terminals
- Run the *_kafka_consumer in a seprate terminal
- Run the *_kafka_producer in a seprate terminal

# Running Spark independently
- Uncomment the `if __name__ == "__main__":` from each daily_script.py, weekly_script.py and monthly_script.py
- Run daily_script.py or weekly_script.py or monthly_script.py as per need

# Running Airflow independently
- Airflow does not run on Windows machine, so follow the guide to setup Airflow in Docker
- Once you have Airflow setup, comment out the `if __name__ == "__main__":` part from each file
- Launch Airflow
- Go to `http://localhost:8080/` and run the DAGs