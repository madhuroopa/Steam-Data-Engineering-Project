# Use Ubuntu 20.04 as the base image
FROM ubuntu:20.04 AS builder-image

# Avoid stuck build due to user prompt
ARG DEBIAN_FRONTEND=noninteractive

# Update and install necessary packages
RUN apt-get update && apt-get install --no-install-recommends -y python3.9 python3.9-dev python3.9-venv python3-pip python3-wheel build-essential curl sudo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create and activate a virtual environment
RUN python3.9 -m venv /home/myuser/venv

# Activate the virtual environment
SHELL ["/bin/bash", "-c"]
RUN source /home/myuser/venv/bin/activate && \
    pip3 install apache-airflow && \
    echo "source /home/myuser/venv/bin/activate" >> /home/myuser/.bashrc

# Copy all folders and files from the current directory to the container
COPY . /home/myuser/code

# Define a new stage for the runner image
FROM ubuntu:20.04 AS runner-image

# Update and install necessary packages for the runner image
RUN apt-get update && apt-get install --no-install-recommends -y python3.9 python3-venv curl sudo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create a new user named 'myuser'
RUN useradd --create-home myuser && echo 'myuser ALL=(ALL:ALL) NOPASSWD:ALL' >> /etc/sudoers

# Set user 'myuser' as sudo user to grant root access
USER myuser

# Copy the virtual environment from the builder image
COPY --from=builder-image /home/myuser/venv /home/myuser/venv

# Copy all folders and files from the builder image to the runner image
COPY --from=builder-image /home/myuser/code /home/myuser/code

# Create a directory for the code
WORKDIR /home/myuser/code

# Expose port 8080 for Apache Airflow web UI (you can modify this if needed)
EXPOSE 8080

# Make sure all messages always reach the console
ENV PYTHONUNBUFFERED=1

# Start Apache Airflow
CMD ["bash", "-c", "source /home/myuser/venv/bin/activate && sudo -E airflow webserver --port 8080 --host 0.0.0.0"]
