
# ETL Project for Loan Default Prediction
## Overview

This project is designed to perform ETL (Extract, Transform, Load) operations for loan default prediction using data from various sources. The project integrates data from Kafka topics, processes it through transformation pipelines, and prepares it for further analysis and visualization.

## Features

- Data Extraction: Consume messages from Kafka topics containing loan data in CSV, Parquet, and JSON formats.

- Data Transformation: Clean, filter, and prepare data for analysis using a robust transformation pipeline.

- Data Loading: Save transformed data into SQL database for further use and analysis.

- Logging: Detailed logging of data consumption, transformation processes, and any errors encountered during the ETL pipeline.

- Workflow Orchestration: Utilize Apache Airflow for scheduling and orchestrating ETL tasks. The project includes an Airflow DAG (loan_default_dag.py) that automates the execution of the ETL pipeline, allowing for efficient and reliable data processing.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.

### Setup

- Clone the Repository
```bash
  git clone https://github.com/PruthviPatel2001/Loan-Default-Risk-Management.git
```

- Start Services
```bash
docker-compose up -d
```

- Configure Apache Airflow: Access the Airflow web UI at http://localhost:8080 to monitor and manage your ETL workflows.

- Run the ETL Pipeline: Once the services are up and running, the Airflow DAG (loan_default_dag.py) will be scheduled to execute the ETL pipeline based on the configuration.


## Testing Mode
To test with a limited dataset, set the test_mode variable to True in kafkaconsumer.py

## Current Working Branch
This repository's current working branch is test2. Make sure to switch to this branch for the latest updates and changes.

## Contributing

I welcome contributions to this project. Please submit a pull request for any improvements or new features. For major changes, consider opening an issue first to discuss the proposed modifications.

- Fork the repository.

- Create a new branch (git checkout -b feature/YourFeature).

- Make your changes and commit (git commit -am 'Add new feature').

- Push to the branch (git push origin feature/YourFeature).

- Create a new Pull Request.
