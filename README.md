# stocks ETL Pipeline Orchestration

### Objective
Execute stocks ETL Spark jobs in Azure Elastic Clusters such as azure databricks and implement a job status tracker in azure postgres database. 

### Pipeline design

Below is the snapshot of the pipeline design in datafactory

![img1](https://github.com/bsathyamur/stocksETL-pipelineOrchestration/blob/main/adf_log_execution.png)

There are two notebooks (stocksETL1 & stocksETL2) in azure databricks workspace scheduled to run in sequence. Notebook process the input files (csv and json) and process the input files and store it in partition folders for trade,stocks and bad input files. The second notebook picks up the files from the parititon trade folder, calculates moving average for current and previos date and merge the data with the quotes input files and stores it in the analytical folder in blob container.

The jobTracker module will log the status of the job in the postgres database table spark_log.

![img2](https://github.com/bsathyamur/stocksETL-pipelineOrchestration/blob/main/postgres_db_logging.png)

The jobTracker.py is stored in the databricks filesystem folder which will be used by the cluster during execution.

![img3](https://github.com/bsathyamur/stocksETL-pipelineOrchestration/blob/main/dbfs_file_load.png)

### Output

BLOB file folder

![img4](https://github.com/bsathyamur/stocksETL-pipelineOrchestration/blob/main/blob_output.png)
