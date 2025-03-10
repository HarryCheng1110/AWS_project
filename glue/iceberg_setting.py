import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
import traceback

### settings
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# iceberg spark config
spark = SparkSession.builder\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")\
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")\
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://my_bucket/")\
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    print("Glue ETL")
    job.commit()
except:
    print(f"Error")
    raise Exception(f"Failed")
