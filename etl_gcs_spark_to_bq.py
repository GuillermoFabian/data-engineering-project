from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_shell import shell_run_command


# @task(retries=3)
# def extract_from_gcs(year: int, month: int) -> Path:
#     """Download trip data from GCS"""
#     gcs_path = f"data/{year}/ny_bike_data_{year}-{month:02}.csv"
#     gcs_block = GcsBucket.load("de-project-bucket")
#     gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
#     return Path(f"../data/{gcs_path}")

#
# @task()
# def quality(path: Path) -> pd.DataFrame:
#     """Data check"""
#     df = pd.read_csv(path)
#     print(f"pre: missing user_type count: {df['usertype'].isna().sum()}")
#     #df["passenger_count"].fillna(0, inplace=True)
#     print(f"post: missing user_type count: {df['usertype'].isna().sum()}")
#
#     return df

@flow()
def spark_tranformation_load_bq():
    command = '''  
    gcloud dataproc jobs submit pyspark \
    --cluster=dataengineering-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dt-project-bucket/code/spark_big_query.py \
    -- \
        --input_2017=gs://dt-project-bucket/data/2017/*/ \
        --input_2018=gs://dt-project-bucket/data/2018/*/ \
        --output=de_project_db.aggr_2015_2016  
    '''

    return shell_run_command(command=command, return_all=True)



# @task(log_prints=True)
# def write_bq(df: pd.DataFrame) -> None:
#     """Write DataFrame to BiqQuery"""
#
#     gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
#
#     df.to_gbq(
#         destination_table="dezoomcamp.rides",
#         project_id="modular-rex-375712",
#         credentials=gcp_credentials_block.get_credentials_from_service_account(),
#         chunksize=500_000,
#         if_exists="append",
#     )
#     print(f"rows: {len(df)}")

spark_tranformation_load_bq()

# @flow()
# def etl_gcs_to_bq():
#     """Main ETL flow to load data into Big Query"""
#
#
#     year = 2017
#     month = 1
#     #
#     # for year in years:
#     #     for month in months:
#
#     path = extract_from_gcs(year, month)
#
#
#     spark_tranformation_load_bq(path)
#
#     #write_bq(df)
#
#
# if __name__ == "__main__":
#     etl_gcs_to_bq()