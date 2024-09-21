**Project Overview**

This project simulates a big data environment by loading a CSV file daily, automating the ETL (Extract, Transform, Load) pipeline using Apache Airflow, performing data transformations using Apache Spark, and storing the results in a PostgreSQL database. The data is then loaded into Power BI to create an interactive dashboard for insights and reporting.

**Key Features**

- *Big Data Simulation:* A large CSV file is treated as big data, loaded into the system on a daily basis.
- *Automation with Airflow:* Scheduling and automating the daily loading and processing tasks using Apache Airflow.
- *Data Transformation with Spark:* Efficiently handling data processing and transformation at scale using PySpark.
- *PostgreSQL Integration:* Transformed data is saved into a PostgreSQL server for storage and querying.
- *Power BI Dashboard:* Interactive data visualization for end users to explore key insights.

**Tech Stack**

- Apache Airflow: Workflow orchestration and automation of ETL pipelines.
- Apache Spark (PySpark): Distributed data processing and transformation.
- PostgreSQL: Relational database for structured data storage.
- Power BI: Data visualization and dashboarding.
- Python: For automation and scripting.

**Workflow**

*Data Simulation:*
- A large CSV file is generated and treated as big data. The file is updated or simulated as new data on a daily basis.
*ETL Automation with Airflow:*
- Apache Airflow automates the daily ingestion of the CSV file, and orchestrates the entire ETL process from data extraction to transformation and loading.
*Data Transformation with Spark:*
- PySpark performs transformations such as data cleaning, aggregations, and other preprocessing tasks on the daily CSV data.
*Data Storage in PostgreSQL:*
- Transformed data is loaded into a PostgreSQL database, providing a reliable, scalable, and queryable storage solution.
*Data Visualization with Power BI:*
- The final dataset is loaded into Power BI for interactive visualization, allowing users to explore the data with charts, filters, and other insights.

**Power BI Dashboard**
- The Power BI dashboard allows end users to interactively explore data insights, track key metrics, and visualize trends over time.
![image](https://github.com/user-attachments/assets/5812ef95-d135-4d03-9b14-6006b6b4eaf2)



**Future Enhancements**
- Add Docker support for simplified environment setup.
- Migrate to a cloud-based data warehouse for improved scalability.
- Implement CI/CD for seamless pipeline deployment and monitoring.

**Conclusion**

This project demonstrates a complete data pipeline from data simulation to interactive dashboarding, showcasing proficiency in automation, data processing, and visualization tools. It highlights critical skills required for data engineering and data analy
