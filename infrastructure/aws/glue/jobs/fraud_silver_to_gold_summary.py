import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Read Silver table from Glue catalog
AmazonS3_node = glueContext.create_dynamic_frame.from_catalog(
    database="fraud_pipeline",
    table_name="silver_transactions",
    transformation_ctx="AmazonS3_node"
)

# SQL aggregation — filter dq_passed=true, aggregate by date and category
SqlQuery = '''
SELECT
    event_date,
    merchant_category,
    COUNT(*)                                           AS total_transactions,
    SUM(CAST(_is_fraud_label AS INT))                  AS fraud_count,
    ROUND(AVG(CAST(_is_fraud_label AS DOUBLE))*100, 3) AS fraud_rate_pct,
    ROUND(AVG(amount), 2)                              AS avg_amount,
    ROUND(MAX(amount), 2)                              AS max_amount,
    ROUND(SUM(amount), 2)                              AS total_amount,
    SUM(CAST(is_online AS INT))                        AS online_count,
    SUM(CAST(is_high_risk_country AS INT))             AS high_risk_country_count
FROM myDataSource
WHERE dq_passed = true
GROUP BY event_date, merchant_category
'''

SQLQuery_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={"myDataSource": AmazonS3_node},
    transformation_ctx="SQLQuery_node"
)

# Evaluate data quality on output
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Write Gold summary to S3 and update Glue catalog
GoldOutput = glueContext.getSink(
    path="s3://fraud-signal-pipeline-371971792187-us-east-1-an/gold/fraud_summary/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["event_date"],
    enableUpdateCatalog=True,
    transformation_ctx="GoldOutput"
)
GoldOutput.setCatalogInfo(
    catalogDatabase="fraud_pipeline",
    catalogTableName="gold_fraud_summary"
)
GoldOutput.setFormat("glueparquet", compression="snappy")
GoldOutput.writeFrame(SQLQuery_node)

job.commit()