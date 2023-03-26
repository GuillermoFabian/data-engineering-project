import datetime
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_shell import shell_run_command
from prefect_shell import ShellOperation


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    print(df.columns)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.head(2))
    df.columns = df.columns.str.replace(" ", "")
    df.columns = map(str.lower, df.columns)

    df["starttime"] = pd.to_datetime(df["starttime"])
    df["stoptime"] = pd.to_datetime(df["stoptime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, year: int, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{year}/{dataset_file}.csv")
    df.to_csv(path)
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-project-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task()
def spark_tranformation_load_bq() -> None:
    command = '''  
    gcloud dataproc jobs submit pyspark \
    --cluster=dataengineering-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dt-project-bucket/code/spark_big_query.py \
    -- \
        --input_2017=gs://dt-project-bucket/data/2017/*/ \
        --input_2018=gs://dt-project-bucket/data/2018/*/ \
        --output=de_project_db.aggr_2017_2018  
    '''
    ShellOperation(
        commands=[
            command
        ]
    ).run()
    #shell_run_command(command=command, return_all=True)

    return


@flow()
def etl_web_gcs_spark(year, dataset_url, dataset_file) -> None:
    """The main ETL function"""

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, year, dataset_file)
    write_gcs(path)

@flow
def etl_parent_flow_web_gcs_spark(catchup_load : bool, years: list[int], target_month: int):
    if catchup_load:
        months = range(1, target_month)
    else:
        years = [int(datetime.datetime.now().strftime('%Y'))]
        months = [int(datetime.datetime.now().strftime('%m'))]

    for year in years:
        for month in months:
            dataset_file = f"ny_bike_data_{year}-{month:02}"
            dataset_url = f"https://s3.amazonaws.com/tripdata/{year}{month:02}-citibike-tripdata.csv.zip"
            etl_web_gcs_spark(year,dataset_url, dataset_file)



    spark_tranformation_load_bq()





if __name__ == "__main__":
    catchup_load = True
    years = [2017, 2018]
    target_month = 12 + 1
    etl_parent_flow_web_gcs_spark(catchup_load, years, target_month)