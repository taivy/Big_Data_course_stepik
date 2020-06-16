import io
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, first, min, max, unix_timestamp, count, sum


def create_additional_columns(df):
    """Creates dataframe with additional columns added"""
    
    df = df.withColumn('time', unix_timestamp(col('date')))
    df = df.withColumn('is_cpm', col('ad_cost_type') == 'CPM')
    df = df.withColumn('is_cpc', col('ad_cost_type') == 'CPC')
    df = df.withColumn('is_view', (col('event') == 'view').cast(IntegerType()))
    df = df.withColumn('is_click', (col('event') == 'click').cast(IntegerType()))
    return df


def create_agg(df):
    """Creates and returns dataframe with aggregated data"""

    seconds_in_day = 60*60*24
    
    df = df.groupBy('ad_id').agg(
        first("target_audience_count").alias("target_audience_count"),
        first("has_video").alias("has_video"),
        first("ad_cost").alias("ad_cost"),
        first("is_cpm").alias("is_cpm"),
        first("is_cpc").alias("is_cpc"),
        (sum(col('is_click')) / sum(col('is_view'))).alias('ctr'),
        ((max("time") - min("time")) / seconds_in_day).alias("day_count"),    
    )
    return df


def process(spark, input_file, target_path):
    df = spark.read.parquet(input_file)
    
    # modify dataframe
    df = create_additional_columns(df)
    df = create_agg(df)
    
    splits = df.randomSplit([0.5, 0.25, 0.25], 42)

    # write results
    splits[0].write.parquet("result/train")
    splits[1].write.parquet("result/test")
    splits[2].write.parquet("result/validate")


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are required.")
    else:
        main(arg)
