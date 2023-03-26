# Data Engineering Project

## Goal
Create dashboard in Google Data studio that can show the 
evolution of NY Bikes Sharing System in 2017 and 2018. 
Also keep the pipeline in schedule so it could be 
scheduled for monthly data loading.
In order to accomplish this we need to first extract the 
data from the Ny Bike Sharing website to a GCB and then 
we will aggregate the data into daily values by running a PySpark Job.

## Configuration:
- Create virtualenv and install requirements.txt
- Create a Data Lake in GCP
- Get credentials from GCP in json file
- Create Bucket - create blocks.py script. Run in terminal:  python blocks.py
- Setting up a Dataproc Cluster in GCP with temp bucket.
https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/code
- Install Google Cloud CLI
https://cloud.google.com/sdk/docs/install-sdk

## 1) Download data with Prefect to Data Lake and schedule for daily runs
https://s3.amazonaws.com/tripdata/index.html
## 2) Submit spark job with Prefect to transform the table with google sdk 
- Upload spark_big_query.py to google cloud bucket

![alt text](/img/spark_code.png)

- Submit job with Prefect
## 3) Add date partition in Spark Job
Column = 'duration_day'
## 4) Create Dashboard 
- Connect Google Data Studio to the Data Warehouse
https://lookerstudio.google.com/reporting/0c4472d8-1ffb-40d5-a4ed-bb5608f6548b
## 5) Schedule for future months
- Deploy with date parametrized variables



