import argparse
import os

from pyspark.sql import SparkSession
import logging


def createTable(spark):

    base_ddlString = f"""create or replace table glue_catalog.{base_glue_schema}.{base_table_name} ( 
        PET_PROFILE_ID Decimal(38,0),
        CUSTOMER_ID Decimal(38,0),
        SUBMITTED_BY_CUSTOMER BOOLEAN,
        SUBMITTED_BY_LOGON STRING,
        PET_NAME STRING,
        PET_TYPE STRING,
        PET_BREED STRING,
        PET_BREED_SIZE_TYPE STRING,
        GENDER STRING,
        WEIGHT_TYPE STRING,
        SIZE_TYPE STRING,
        BIRTHDAY DATE,
        BIRTHDAY_ESTIMATED BOOLEAN,
        LIFE_STAGE STRING,
        ADOPTED BOOLEAN,
        ADOPTION_DATE DATE,
        STATUS STRING,
        STATUS_REASON STRING,
        TIME_CREATED TIMESTAMP,
        TIME_UPDATED TIMESTAMP,
        WEIGHT Decimal(38,0),
        ALLERGY_COUNT Decimal(38,0),
        PHOTO_COUNT Decimal(38,0),
        MKTG_SAFE BOOLEAN,
        PET_BREED_ID Decimal(38,0),
        PET_TYPE_ID Decimal(38,0),
        PET_NEW BOOLEAN,
        FIRST_BIRTHDAY DATE,
        SEVEN_BIRTHDAY DATE,
        LEGAL_COMPANY_DESCRIPTION STRING,
        WEIGHT_UOM STRING,
        WEIGHT_METRIC Decimal(18,6),
        WEIGHT_METRIC_UOM STRING,
        REPLICATION_SYNCED DATE,
	    REPLICATION_DELETED_FLAG BOOLEAN)
    using iceberg
    location '{base_table_location}' """
    
    try:
        spark.sql(base_ddlString)

    except Exception as e:
        logging.error(e)
        raise Exception

    finally:
        spark.stop()

if __name__=="__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--env", help="AWS Environment")
    parser.add_argument("--aws-short-region", help="AWS Short Region Name")
    parser.add_argument("--region-name", help="AWS Full Region Name")

    args = parser.parse_args()
    env = args.env
    aws_short_region = args.aws_short_region
    region_name = args.region_name
    
    glue_catalog = "s3://{0}-{1}-data-lake-pipeline-catalog/glue_catalog/".format(env, aws_short_region)
    base_table_name = 'pet_profile'
    base_glue_schema= f"{env}_{aws_short_region}_cdm_gold_schema"

    base_table_location = os.path.join(glue_catalog , base_glue_schema ,base_table_name)

    spark = SparkSession.builder.\
                appName("ddl-deployment-for-cdm_attrep_apply_exception").\
                config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").\
                config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog"). \
                config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog").\
                config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO").\
                config("spark.sql.catalog.glue_catalog.warehouse",glue_catalog).\
                getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    createTable(spark)
