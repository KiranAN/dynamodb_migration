
import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, to_timestamp, lit
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DELTA_START_UTC", "DELTA_END_UTC"]
)

SOURCE_TABLE = "multitenant-async-api-dev-records-TENANT_A"
DEST_TABLE = "tenant-a-migration-destination_glue"
EXPECTED_TENANT = "TENANT_A"

DELTA_START_UTC = args["DELTA_START_UTC"]
DELTA_END_UTC = args["DELTA_END_UTC"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read source DynamoDB table
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": SOURCE_TABLE,
        "dynamodb.throughput.read.percent": "0.5"
    },
    transformation_ctx="DynamoDBSource"
)

df = source_dyf.toDF()

print("Source schema:")
df.printSchema()

# Validate required columns
required = ["id", "tenant_id", "created_at"]

for field in required:
    if field not in df.columns:
        raise Exception(f"Missing required field: {field}")

# Apply delta timestamp window and tenant filter
delta_df = df.filter(
    (col("tenant_id") == EXPECTED_TENANT)
    & (to_timestamp(col("created_at")) >=
       to_timestamp(lit(DELTA_START_UTC)))
    & (to_timestamp(col("created_at")) <
       to_timestamp(lit(DELTA_END_UTC)))
).cache()

delta_count = delta_df.count()

print("Delta window start:", DELTA_START_UTC)
print("Delta window end:", DELTA_END_UTC)
print("Delta records found:", delta_count)

if delta_count == 0:
    print("No records found in this delta window.")

else:
    # Validate IDs and tenant ownership
    invalid_df = delta_df.filter(
        col("id").isNull()
        | (trim(col("id").cast("string")) == "")
        | (col("tenant_id") != EXPECTED_TENANT)
    )

    invalid_count = invalid_df.count()

    if invalid_count:
        invalid_df.show(20, truncate=False)
        raise Exception(f"Invalid records: {invalid_count}")

    # Validate duplicate IDs
    distinct_count = delta_df.select("id").distinct().count()

    if distinct_count != delta_count:
        raise Exception("Duplicate IDs found in delta records")

    # Write delta records
    output_dyf = DynamicFrame.fromDF(
        delta_df,
        glueContext,
        "MigrationDeltaOutput"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="dynamodb",
        connection_options={
            "dynamodb.output.tableName": DEST_TABLE,
            "dynamodb.throughput.write.percent": "0.5"
        },
        transformation_ctx="DynamoDBDeltaSink"
    )

    print("Delta migration completed.")
    print("Records processed:", delta_count)

# Commit exactly once
delta_df.unpersist()
job.commit()
