import os
import sys
import json
import logging
from datetime import datetime, timedelta

import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as f, types as t

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection, Filter

# configure logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

# get the arguments for the job
try:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'raw-data-bucket',
        'processed-data-bucket',
        'database-name',
        'rds-endpoint',
        'rds-port',
        'rds_username',
        'rds_password',
        'rds-database'
    ])
except Exception:
    args = {
        'JOB_NAME': os.environ.get('JOB_NAME', 'local-job'),
        'raw-data-bucket': os.environ.get('raw-data-bucket'),
        'processed-data-bucket': os.environ.get('processed-data-bucket'),
        'database-name': os.environ.get('database-name') or os.environ.get('database_name'),
        'rds-endpoint': os.environ.get('rds-endpoint') or os.environ.get('rds_endpoint'),
        'rds-port': os.environ.get('rds-port') or os.environ.get('rds_port'),
        'rds_username': os.environ.get('rds_username'),
        'rds_password': os.environ.get('rds_password'),
        'rds-database': os.environ.get('rds-database') or os.environ.get('rds_database'),
    }

# prevent KeyError issue
def get_arg(key: str, default_value = None, required: bool = False):
    if key in args and args[key] not in (None, ""):
        return args[key]

    # try environment fallbacks
    env_key_hyphen = key
    env_key_underscore = key.replace('-', '_')
    value = os.environ.get(env_key_hyphen)
    if value in (None, ""):
        value = os.environ.get(env_key_underscore, default_value)
    if required and (value in (None, "")):
        raise KeyError(f"Missing required argument: {key}")
    return value

# initialize Glue context
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(get_arg('JOB_NAME', 'local-job'), args)

# initialize boto3 client for secret manager
secretsmanager_client = boto3.client('secretsmanager')
spark.conf.set("spark.sql.shuffle.partitions", "8")

logger.info("Start NYC Taxi ETL Job")
logger.info(f"Args received: {list(args.keys())}")

# get the password  from secret manager
def get_database_password():
    try:
        secret_arn = get_arg('rds_password', required = True)
        response = secretsmanager_client.get_secret_value(SecretId = secret_arn)
        secret = json.loads(response['SecretString'])
        return secret['password']
    except Exception as e:
        logger.error(f"Failed to retrieve password from Secret Manager: {str(e)}")
        raise e

# create MySQL connection to write the processed data to RDS 
def mysql_connection():
    db_password = get_database_password()
    return {
        "url": f"jdbc:mysql://{get_arg('rds_endpoint', required = True)}:{get_arg('rds_port', required = True)}/{get_arg('rds_database', required = True)}",
        "dbtable": "taxi_trips",
        "user": get_arg('rds_username', required = True),
        "password": db_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

# apply data quality rules to check data quality
def data_quality_rules(dynamic_frame):
    try:
        logger.info("Start data quality check")
        src_df = dynamic_frame.toDF()
        src_df = src_df.withColumn("__row_id", f.monotonically_increasing_id())

        # determine whether the record can pass
        row_pass_expr = (
            f.col("vendorid").isNotNull() &
            f.col("tpep_pickup_datetime").isNotNull() &
            f.col("tpep_dropoff_datetime").isNotNull() &
            f.col("passenger_count").isNotNull() &
            f.col("trip_distance").isNotNull() &
            f.col("fare_amount").isNotNull() &
            (f.col("passenger_count") >= 1) & (f.col("passenger_count") <= 9) &
            (f.col("trip_distance") >= 0) & (f.col("trip_distance") <= 1000) &
            (f.col("fare_amount") >= 0) & (f.col("fare_amount") <= 1000)
        )

        passed_ids = (src_df
                    .withColumn("__row_pass", row_pass_expr.cast("boolean"))
                    .filter(f.col("__row_pass"))
                    .select("__row_id"))

        # keep the passed records
        out_df = (src_df
                .join(passed_ids, on = "__row_id", how = "inner") \
                .drop("__row_id", "__row_pass")
                )

        total_rows = src_df.count()
        passed_rows = out_df.count()
        failed_rows = total_rows - passed_rows
        logger.info(f"DQ total_rows = {total_rows}, passed_rows = {passed_rows}, "
                    f"failed_rows = {failed_rows}")

        # execute DQDL for cloudwatch
        try:
            rules_str = """
            Rules = [
              IsComplete "vendorid",
              IsComplete "tpep_pickup_datetime",
              IsComplete "tpep_dropoff_datetime",
              IsComplete "passenger_count",
              IsComplete "trip_distance",
              IsComplete "fare_amount",
              (ColumnValues "passenger_count" >= 1) and (ColumnValues "passenger_count" <= 9),
              (ColumnValues "trip_distance" >= 0) and (ColumnValues "trip_distance" <= 1000),
              (ColumnValues "fare_amount" >= 0) and (ColumnValues "fare_amount" <= 1000)
            ]
            """
            _ = EvaluateDataQuality().process_rows(
                frame = dynamic_frame,                 
                ruleset = rules_str,
                publishing_options = {
                    "dataQualityEvaluationContext": f"{args['JOB_NAME']}-dq-evaluation",
                    "enableDataQualityCloudWatchMetrics": True,
                    "enableDataQualityResultsPublishing": True,
                },
                additional_options={
                    "performanceTuning.caching": "CACHE_NOTHING",
                    "compositeRuleEvaluation.method": "ROW",
                    "observations.scope": "ALL"
                },
            )
        except Exception as e:
            logger.warning(f"DQ metrics publishing skipped: {e}")

        return DynamicFrame.fromDF(out_df, glue_context, "dq_passed_rows")

    except Exception as e:
        logger.error(f"Data quality evaluation failed: {e}")
        logger.warning("Continuing with ETL job despite data quality check failure")
        return dynamic_frame

# enrich the data with geographic information using location IDs
def geographic_data(dynamic_frame):
    logger.info("Enrich data with geographic information")

    try:
        '''
        zone_lookup_df = glue_context.create_dynamic_frame.from_catalog(
            database = get_arg('database_name', required = True),
            table_name = "reference_data",
            transformation_ctx = "zone_lookup_df",
        ).toDF()
        '''

        raw_bucket = args.get('raw-data-bucket') or args.get('raw_data_bucket')
        zones_path = f"s3://{raw_bucket}/reference-data/"
        zone_lookup_df = (spark.read.option('header','true').option('inferSchema','true').csv(zones_path))

        logger.info(f"Zone lookup records loaded")

        # convert the main data to DF
        df = dynamic_frame.toDF()

        # join with the zone lookup data using pickup location IDS
        df_pickup = df.alias("trips").join(
            zone_lookup_df.alias("pickup_zones"),
            f.col("trips.PULocationID") == f.col("pickup_zones.LocationID"), "left"
        ).select(
            f.col("trips.*"),
            f.col("pickup_zones.Borough").alias("pickup_borough"),
            f.col("pickup_zones.Zone").alias("pickup_zone"),
            f.col("pickup_zones.service_zone").alias("pickup_service_zone"),
        )
        
        # join with the zone lookup data using dropoff location IDS
        df_enriched = df_pickup.alias("trips_pickup").join(
            zone_lookup_df.alias("dropoff_zones"),
            f.col("trips_pickup.DOLocationID") == f.col("dropoff_zones.LocationID"), "left"
        ).select(
            f.col("trips_pickup.*"),
            f.col("dropoff_zones.Borough").alias("dropoff_borough"),
            f.col("dropoff_zones.Zone").alias("dropoff_zone"),
            f.col("dropoff_zones.service_zone").alias("dropoff_service_zone")
        )

        logger.info(f"Completes the geographic information enrichment")

        return DynamicFrame.fromDF(df_enriched, glue_context, "enriched_data")

    except Exception as e:
        logger.warning(f"Geographic information enrichment failed: {str(e)}")
        logger.info("Continue without geographic data")
        return dynamic_frame

# transform the nyc taxi data
def transform_data(dynamic_frame):
    logger.info("Start data transformation")
    df = dynamic_frame.toDF()
    df_cleaned = (df
                .dropDuplicates()
                .withColumn("tpep_pickup_datetime",  f.to_timestamp("tpep_pickup_datetime"))
                .withColumn("tpep_dropoff_datetime", f.to_timestamp("tpep_dropoff_datetime"))
                .filter(f.col("tpep_dropoff_datetime") >= f.col("tpep_pickup_datetime"))
    )

    df_transformed = (
    df_cleaned
    .withColumn(
        "trip_duration_min",
        (f.unix_timestamp(f.col("tpep_dropoff_datetime")) - f.unix_timestamp(f.col("tpep_pickup_datetime"))) / 60.0
    )
    .withColumn(
        "base_fare_per_mile",
        f.when(f.col("trip_distance") > 0, f.col("fare_amount") / f.col("trip_distance")).otherwise(0)
    )
    .withColumn(
        "fare_per_mile",
        f.when(f.col("trip_distance") > 0, f.col("total_amount") / f.col("trip_distance")).otherwise(0)
    )
    .withColumn("pickup_year", f.year("tpep_pickup_datetime"))
    .withColumn("pickup_month", f.month("tpep_pickup_datetime"))
    .withColumn("pickup_day", f.dayofmonth("tpep_pickup_datetime"))
    .withColumn("pickup_hour", f.hour("tpep_pickup_datetime"))
    .withColumn("pickup_dayofweek", f.dayofweek("tpep_pickup_datetime"))
    .withColumn(
        "is_weekend",
        f.when(f.col("pickup_dayofweek").isin(1, 7), 1).otherwise(0)
    )
    .withColumn(
        "time_of_day",
        f.when(f.col("pickup_hour").between(6, 11),  "Morning")
         .when(f.col("pickup_hour").between(12, 17), "Afternoon")
         .when(f.col("pickup_hour").between(18, 23), "Evening")
         .otherwise("Night")
    )
    )

    return DynamicFrame.fromDF(df_transformed, glue_context, "transformed_data")

# create aggregation for analysis
def create_aggregated_views(df):
    logger.info("Create aggregation for analysis")

    # daily aggregations
    daily_stats = df.groupBy("pickup_year", "pickup_month", "pickup_day", "VendorID").agg(
        f.count("*").alias("trip_count"),
        f.avg(f.col("trip_duration_min")).alias("avg_trip_duration"),
        f.avg(f.col("base_fare_per_mile")).alias("avg_base_fare_per_mile"),
        f.avg(f.col("fare_per_mile")).alias("avg_fare_per_mile"),
        f.avg(f.col("fare_amount")).alias("avg_fare_amount"),
        f.avg(f.col("trip_distance")).alias("avg_trip_distance"),
        f.sum(f.col("fare_amount")).alias("base_revenue"),
        f.sum(f.col("total_amount")).alias("gross_revenue")
    )


    # hourly aggregations
    hourly_stats = df.groupBy("pickup_year", "pickup_month", "pickup_day", "pickup_hour").agg(
        f.count("*").alias("trip_count"),
        f.avg(f.col("trip_duration_min")).alias("avg_trip_duration"),
        f.avg(f.col("base_fare_per_mile")).alias("avg_base_fare_per_mile"),
        f.avg(f.col("fare_per_mile")).alias("avg_fare_per_mile")
    )

    # vendor performance aggregations
    vendor_stats = df.groupBy("VendorID").agg(
        f.count("*").alias("trip_count"),
        f.avg(f.col("trip_duration_min")).alias("avg_trip_duration"),
        f.avg(f.col("base_fare_per_mile")).alias("avg_base_fare_per_mile"),
        f.avg(f.col("fare_per_mile")).alias("avg_fare_per_mile"),
        f.avg(f.col("fare_amount")).alias("avg_fare_amount"),
        f.stddev(f.col("base_fare_per_mile")).alias("base_fare_per_mile_stddev"),
        f.stddev(f.col("fare_per_mile")).alias("fare_per_mile_stddev")
    )

    return daily_stats, hourly_stats, vendor_stats

# write the transformed data to MySQL RDS
def write_to_sql(dynamic_frame, table_name):
    logger.info(f"Write to MySQL table: {table_name}")

    connection_options = mysql_connection()
    connection_options["dbtable"] = table_name

    # write to MySQL
    glue_context.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "jdbc",
        connection_options = {
            "url": connection_options["url"],
            "dbtable": connection_options["dbtable"],
            "user": connection_options["user"],
            "password": connection_options["password"],
            "driver": connection_options["driver"],
        },
        transformation_ctx = f"write_{table_name}"
    )

    logger.info(f"Successfully write data to {table_name}")

# the ETL process
def main():
    try:
        logger.info("Read raw data from S3 bucket")
        raw_data_bucket = get_arg('raw-data-bucket', default_value = None, required = False)
        raw_data_path = f"s3://{raw_data_bucket}/data/" if raw_data_bucket else None

        # create dynamic frame
        dyf = glue_context.create_dynamic_frame.from_catalog(
            database = get_arg('database_name', required = True),
            table_name = "data",
            transformation_ctx = "dyf"
        )

        logger.info(f"Raw data loaded. Record count: {dyf.count()}")

        # apply data quality checks and get validated data
        validated_data = data_quality_rules(dyf)

        # apply the geographic information enrichment
        enriched_data = geographic_data(validated_data)

        # transform the validated data
        transformed_data = transform_data(enriched_data)

        # write to MySQL
        write_to_sql(transformed_data, "taxi_trips")

        # create data aggregation
        df = transformed_data.toDF()
        daily_stats, hourly_stats, vendor_stats = create_aggregated_views(df)

        # write the aggregated data
        write_to_sql(DynamicFrame.fromDF(daily_stats, glue_context, "daily_stats"), "daily_trip_stats")
        write_to_sql(DynamicFrame.fromDF(hourly_stats, glue_context, "hourly_stats"), "hourly_trip_stats")
        write_to_sql(DynamicFrame.fromDF(vendor_stats, glue_context, "vendor_stats"), "vendor_performance")

        # write the processed data to S3 bucket
        logger.info("Write processed data to S3")
        processed_bucket = get_arg('processed_data_bucket', default_value = None, required = False)
        if processed_bucket:
            processed_data_path = f"s3://{processed_bucket}/processed-data/"
            glue_context.write_dynamic_frame.from_options(
                frame = transformed_data,
                connection_type = "s3",
                format = "parquet",
                connection_options = {
                    "path": processed_data_path, 
                    "partitionKeys": ["pickup_year", "pickup_month"] 
                },
                transformation_ctx = "write_processed_data"
            )
        else:
            logger.warning("'processed-data-bucket' argument missing; skipping S3 write of processed data.")

        logger.info("ETL job completes successfully")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()