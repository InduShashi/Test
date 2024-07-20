import argparse
import logging
import os

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, to_date

from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType, DateType, DecimalType, TimestampType

def load_iceberg_table(spark, fully_qualified_table_name, source_file_path):
    schema = StructType([
        StructField('PET_PROFILE_ID', DecimalType(38,0), True),
        StructField('CUSTOMER_ID', DecimalType(38,0), True),
        StructField('SUBMITTED_BY_CUSTOMER', BooleanType(), True),
        StructField('SUBMITTED_BY_LOGON', StringType(), True),
        StructField('PET_NAME', StringType(), True),
        StructField('PET_TYPE', StringType(), True),
        StructField('PET_BREED',  StringType(), True),
        StructField('PET_BREED_SIZE_TYPE', StringType(), True),
        StructField('GENDER', StringType(), True),
        StructField('WEIGHT_TYPE', StringType(), True),
        StructField('SIZE_TYPE',  StringType(), True),
        StructField('BIRTHDAY', DateType(), True),
        StructField('BIRTHDAY_ESTIMATED',BooleanType(), True),
        StructField('LIFE_STAGE', StringType(), True),
        StructField('ADOPTED', BooleanType(), True),
        StructField('ADOPTION_DATE', DateType(), True),
        StructField('STATUS', StringType(), True),
        StructField('STATUS_REASON', StringType(), True),
        StructField('TIME_CREATED', TimestampType(), True),
        StructField('TIME_UPDATED', TimestampType(), True),
        StructField('WEIGHT', DecimalType(38,0), True),
        StructField('ALLERGY_COUNT', DecimalType(38,0), True),
        StructField('PHOTO_COUNT', DecimalType(38,0), True),
        StructField('MKTG_SAFE', BooleanType(), True),
        StructField('PET_BREED_ID', DecimalType(38,0), True),
        StructField('PET_TYPE_ID', DecimalType(38,0), True),
        StructField('PET_NEW', BooleanType(), True),
        StructField('FIRST_BIRTHDAY', DateType(), True),
        StructField('SEVEN_BIRTHDAY', DateType(), True),
        StructField('LEGAL_COMPANY_DESCRIPTION', StringType(), True),
        StructField('WEIGHT_UOM', StringType(), True),
        StructField('WEIGHT_METRIC', DecimalType(18,6), True),
        StructField('WEIGHT_METRIC_UOM', StringType(), True)
    ])

    try:
        df=spark.read.format("parquet").schema(schema).option("recursiveFileLookup", True).load(source_file_path).\
        withColumn("REPLICATION_SYNCED", current_date()). \
        withColumn("REPLICATION_DELETED_FLAG", lit(False))
        df.writeTo(fully_qualified_table_name).overwritePartitions()
    except Exception as e:
        logging.info("Pet_Profile Original Base load failed")
        raise Exception
    finally:
        spark.stop()


if __name__=="__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--uuid", help="To avoid duplication")
    parser.add_argument("--env", help="AWS Environment")
    parser.add_argument("--aws-short-region", help="AWS Short Region Name")
    parser.add_argument("--region-name", help="AWS Full Region Name")
    parser.add_argument("--log-stream", help="AWS Full Region Name", required=False)

    args = parser.parse_args()
    uuid = args.uuid
    env = args.env
    aws_short_region = args.aws_short_region
    region_name = args.region_name

    glue_catalog_location = "s3://{0}-{1}-data-lake-pipeline-catalog/glue_catalog/".format(env, aws_short_region)
    glue_catalog_name = "glue_catalog"
    table_name = "pet_profile"
    glue_schema = f"{env}_{aws_short_region}_cdm_gold_schema"
    table_location = os.path.join(glue_catalog_location,glue_schema,table_name)
    fully_qualified_table_name = "{0}.{1}.{2}".format(glue_catalog_name, glue_schema, table_name)

   
    # source_file_path = "s3://{env}-{aws_short_region}-data-lake-pipeline-snowflake/snowflake-data-export/pet_profile/".format(env,aws_short_region)
    source_file_path = "s3://{0}-{1}-data-lake-pipeline-snowflake/snowflake-data-export/pet_profile/{2}/*.parquet".format(env,aws_short_region,uuid)
    spark = SparkSession.builder. \
        appName("cdm-pet-profile-load"). \
        config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"). \
        config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"). \
        config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"). \
        config("spark.sql.catalog.glue_catalog.warehouse", glue_catalog_location). \
        config("spark.sql.sources.partitionOverwriteMode", "dynamic"). \
        config("spark.sql.parquet.enableVectorizedReader", "false").\
        config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED").\
        getOrCreate()

    load_iceberg_table(spark,fully_qualified_table_name, source_file_path)

