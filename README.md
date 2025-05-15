# f1racing-ErgastAPI

F1 Racing Data Project – Azure & Ergast API
This project leverages the Ergast Developer API to extract historical and live Formula 1 racing data and build a scalable data pipeline using Azure services.
 Technologies Used:
•	Ergast API – Source for real-time and historical F1 race data
•	Azure Data Factory (ADF) – Orchestrates data ingestion from the API
•	Azure Databricks (PySpark) – Handles data transformation, cleansing, and enrichment
•	Azure Data Lake – Stores structured data across Bronze, Silver, and Gold layers
•	Delta Lake – Enables scalable, ACID-compliant data storage with versioning
•	Power BI / Azure Synapse (optional) – For dashboards and advanced analytics
 Key Features:
•	Automated data ingestion from Ergast API using Azure Data Factory
•	Data transformation and modeling using Databricks notebooks (PySpark)
•	Implementation of the Medallion Architecture (Raw → Processed → Presentation)
•	Incremental data loads scheduled weekly for new race and driver data
•	Analytics-ready datasets for performance tracking (e.g., drivers, teams, circuits, lap times)
Objective:
To design and implement a modern data engineering solution for sports analytics, demonstrating end-to-end pipeline development using Azure cloud technologies.

