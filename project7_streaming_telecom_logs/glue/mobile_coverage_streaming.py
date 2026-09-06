import sys
import logging
from pyspark.sql import SparkSession
from awsglue.job import Job
from pyspark.sql.functions import (
    window, col, from_json, avg, concat, lit, current_date, 
    to_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from awsglue.context import GlueContext
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

try:
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = SparkSession.builder.appName("KinesisDataAnalysis").getOrCreate()
    logger.info("Spark session created successfully.")
    
    # Define the schema for the new dataset
    schema = StructType([
        StructField("hour", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("signal", StringType(), True),
        StructField("network", StringType(), True),
        StructField("operator", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("satellites", StringType(), True),
        StructField("precission", DoubleType(), True),
        StructField("provider", StringType(), True),
        StructField("activity", StringType(), True),
        StructField("postal_code", StringType(), True)
    ])
    
    # Read from Kinesis
    raw_data_frame = glueContext.create_data_frame.from_options(
        connection_type="kinesis",
        connection_options={
            "streamARN": "arn:aws:kinesis:ap-south-1:<aws-account-id>:stream/mobile_coverage_logs",
            "classification": "json",
            "startingPosition": "trim_horizon",
            "inferSchema": "true"
        }
    )
    
    data_frame = raw_data_frame.select(from_json(col("$json$data_infer_schema$_temporary$"), schema).alias("parsed")).select("parsed.*")
    
    data_frame_processed = data_frame.withColumn(
    "timestamp", 
    to_timestamp(concat
         (current_date().cast("string"), lit(" "), col("hour")), "yyyy-MM-dd HH:mm:ss")
     ).withColumn("partition_hour",date_format(col("timestamp"), "HH"))
    
    data_frame_with_watermark = data_frame_processed.withWatermark("timestamp", "2 minutes")
    
    signal_strength_by_operator_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "postal_code", "operator", "partition_hour"
    ).agg(
        avg("signal").alias("average_signal")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "operator",
        "partition_hour",
        "average_signal"
    )
    
    gps_precision_by_provider_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "provider","partition_hour"
    ).agg(
        avg("precission").alias("average_precision"),
        avg("satellites").alias("average_satellites")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "provider",
        "partition_hour",
        "average_precision",
        "average_satellites"
    )
    
    status_count_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "postal_code", "status", "partition_hour"
    ).count().select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "status",
        "partition_hour",
        col("count").alias("status_count")
    )
    
    s3_path = "s3://dcn-dataengg/telecom_logs/aggregations/"
    s3_path_checkpoint = "s3://dcn-dataengg/telecom_logs/checkpoints/"
    
    signal_strength_by_operator = signal_strength_by_operator_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "signal_strength_by_operator") \
        .option("path", s3_path + "signal_strength_by_operator/") \
        .partitionBy("partition_hour", "postal_code") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    gps_precision_by_provider = gps_precision_by_provider_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "gps_precision_by_provider") \
        .option("path", s3_path + "gps_precision_by_provider/") \
        .partitionBy("partition_hour", "provider") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    status_count = status_count_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "status_count") \
        .option("path", s3_path + "status_count/") \
        .partitionBy("partition_hour","postal_code") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    logger.info("Starting the streaming jobs.")
    signal_strength_by_operator.awaitTermination()
    gps_precision_by_provider.awaitTermination()
    status_count.awaitTermination()
    
except Exception as e:
    logger.error("An error occurred: ", exc_info=True)