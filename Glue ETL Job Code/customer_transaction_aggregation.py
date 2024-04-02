import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import pymysql
import pymysql.cursors

# Connect to the database using db details or fetch these from Glue connections
connection = pymysql.connect(host='host_address',
                             user='user_name',
                             password='password',
                             database='db',
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
        table = "aggregated_transactions"
        fetch_sql = f"select * from {table}"
        cursor.execute(fetch_sql)
        rows = cursor.fetchall()
        truncate_sql = f"truncate table {table}"
        cursor.execute(truncate_sql)
        
    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

# Script for node S3 Daily Data
S3DailyData = glueContext.create_dynamic_frame.from_catalog(
    database="sales_db",
    table_name="s3_input_raw_data",
    transformation_ctx="S3DailyData",
)

s3_df = S3DailyData.toDF()
s3_df.createOrReplaceTempView('s3')
rds_df_persisted = spark.createDataFrame(rows)
rds_df_persisted.createOrReplaceTempView('rds')

final_agg_spark_df = spark.sql("""
select customer_id, debit_card_number, bank_name, sum(total_amount_spent) as total_amount_spent
from
(select customer_id, debit_card_number, bank_name, total_amount_spent from rds
union all
select customer_id, debit_card_number, bank_name, amount_spend as total_amount_spent from s3)
group by customer_id, debit_card_number, bank_name
""")

final_agg_dynamic_frame = DynamicFrame.fromDF(final_agg_spark_df, glueContext, "final_agg_spark_df")

final_insert = glueContext.write_dynamic_frame.from_catalog(frame=final_agg_dynamic_frame, database="sales_db", table_name="rds_db_aggregated_transactions", transformation_ctx="final_insert")

job.commit()