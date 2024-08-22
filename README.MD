# Supermarket Data ETL and Analysis Pipeline

This project involves developing a comprehensive pipeline for extracting, transforming, and analyzing supermarket data. The goal is to streamline the processing of large datasets to extract actionable insights and support decision-making.

# Key Components:

1. **Data Extraction:**

- **Source**: PostgreSQL Database
- **Process**: Extract data from PostgreSQL tables containing sales transactions, inventory records, and customer information. Utilize SQL queries and Python scripts (using libraries like Psycopg2 or SQLAlchemy) for efficient data retrieval.
- **Destination**: Google Cloud Storage (GCS)
- **Process**: Upload the extracted raw data files to Google Cloud Storage buckets. Organize the data for efficient access and further processing using tools such as the Google Cloud SDK or Python's google-cloud-storage library.

2. **Data Cleaning and Transformation:**

- **Process:**
  - **Cleaning:** Remove any inconsistencies, handle missing values, and correct data errors.
  - **Transformation:** Preprocess the data to conform to the required format, apply necessary transformations, and prepare it for analysis.
  - **Tools:** Utilize Python (Pandas, NumPy) for data cleaning and transformation, and consider using Google Cloud Dataflow for scalable processing if needed.

3. **Data Loading and Schema Creation:**

- **Platform:** Google BigQuery
- **Process:**
  - **Loading:** Import the cleaned and transformed data from Google Cloud Storage into BigQuery. Use BigQuery's data import features or Python's google-cloud-bigquery library for the transfer.
  - **Schema Creation:** Define and configure the Star schema in BigQuery to structure the data appropriately for analysis. Ensure that the schema supports the necessary queries and reports.

4. **Data Analysis and Reporting:**

- **Tool:** Power BI
- **Process:** Connect Power BI to Google BigQuery to visualize and analyze the processed data. Create interactive dashboards and reports to display key performance metrics, trends, and insights, such as sales trends, inventory levels, and customer behavior.

# Workflow Orchestration and Containerization:

1. **Apache Airflow:**

- **Purpose:** Orchestrate and automate the data pipeline processes, including data extraction, loading, and transformation.
- **Implementation:** Create Directed Acyclic Graphs (DAGs) to manage the workflow, schedule tasks, and handle dependencies between different stages of the pipeline.

2. **Docker:**

- **Purpose:** Containerize the data pipeline components to ensure consistency and portability across different environments.
- **Implementation:** Use Docker to create containers for the extraction scripts, transformation processes, and any other components, facilitating seamless deployment and scalability.

## Tech Stack

- **Data Extraction and Storage:** PostgreSQL, Python (Psycopg2, SQLAlchemy), Google Cloud Storage
- **Data Cleaning and Transformation:** Python (Pandas, NumPy), Google Cloud Dataflow
- **Data Loading and Schema Creation:** Google BigQuery, SQL, Python (google-cloud-bigquery)
- **Workflow Orchestration:** Apache Airflow
- **Containerization:** Docker
- **Data Analysis and Reporting:** Power BI, Google BigQuery Connector
