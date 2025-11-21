#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, round as spark_round, radians, sin, cos, atan2, sqrt
)
from pyspark.sql.types import IntegerType
import re
import argparse

FOLDER_PATTERN = re.compile(r"^[0-9]{6}$")
CSV_FILE_NAME = "data.csv"

def list_valid_input_csvs(spark, base_path):
    paths = []
    try:
        Path = spark._jvm.org.apache.hadoop.fs.Path
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        base = Path(base_path)
        statuses = fs.listStatus(base)
        for status in statuses:
            name = status.getPath().getName()
            if FOLDER_PATTERN.match(name):
                folder_path = status.getPath().toString()
                csv_path = folder_path + "/" + CSV_FILE_NAME
                if fs.exists(Path(csv_path)):
                    paths.append((name, csv_path))
    except Exception as e:
        raise RuntimeError(f"Failed to access HDFS '{base_path}': {e}")
    if not paths:
        raise RuntimeError(f"No yyyyMM folders '{CSV_FILE_NAME}' found under '{base_path}'")
    return sorted(paths, key=lambda x: x[0])

def main():
    parser = argparse.ArgumentParser(description="Clean Hubway new format files")
    parser.add_argument("--base-path", dest="base_path", required=True, help="Input base path (hdfs) containing yyyyMM folders")
    parser.add_argument("--output-path", dest="output_path", required=True, help="Output parquet path (hdfs) for cleaned data")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Hubway ETL")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    base_path = args.base_path
    output_path = args.output_path

    csv_files = list_valid_input_csvs(spark, base_path)
    df_list = []

    for ym, csv_path in csv_files:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"')
            .option("escape", "\\")
            .csv(csv_path)
        )

        df = (
            df.withColumn("trip_duration_min", spark_round(col("tripduration") / 60.0, 2))
            .withColumn("trip_start_ts", to_timestamp(col("starttime")))
            .withColumn("trip_end_ts", to_timestamp(col("stoptime")))
            .withColumn("birth_year_raw", col("birth year"))
            .withColumn(
                "birth_year",
                when(col("birth_year_raw").isNull(), None)
                .when(col("birth_year_raw") == "\\N", None)
                .when(col("birth_year_raw") == "0", None)
                .otherwise(col("birth_year_raw").cast(IntegerType()))
            )
            .withColumn(
                "gender",
                when(col("gender") == "\\N", None)
                .otherwise(col("gender").cast(IntegerType()))
            )
            .withColumn("year_month", lit(ym))
        )

   
        R = 6371000.0  # radius for haversine calculation

        df = df.withColumn("lat1", radians(col("start station latitude"))) \
               .withColumn("lon1", radians(col("start station longitude"))) \
               .withColumn("lat2", radians(col("end station latitude"))) \
               .withColumn("lon2", radians(col("end station longitude")))

        df = df.withColumn("dlat", col("lat2") - col("lat1")) \
               .withColumn("dlon", col("lon2") - col("lon1"))

        df = df.withColumn(
            "a",
            sin(col("dlat") / 2) ** 2
            + cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon") / 2) ** 2
        ).withColumn(
            "c",
            2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))
        ).withColumn(
            "trip_distance_m",
            spark_round(R * col("c"), 2)
        )

        df = df.drop("lat1", "lon1", "lat2", "lon2", "dlat", "dlon", "a", "c", "birth_year_raw")

        df = df.select(
            "trip_duration_min",
            "trip_start_ts",
            "trip_end_ts",
            col("start station id").alias("start_station_id"),
            col("start station name").alias("start_station_name"),
            col("end station id").alias("end_station_id"),
            col("end station name").alias("end_station_name"),
            col("bikeid").alias("bike_id"),
            "birth_year",
            "gender",
            "trip_distance_m",
            "year_month",
        )

        df_list.append(df)

    final_df = df_list[0]
    for df in df_list[1:]:
        final_df = final_df.unionByName(df)

    (
        final_df.write.mode("overwrite")
        .partitionBy("year_month")
        .parquet(output_path)
    )

    spark.stop()

if __name__ == "__main__":
    main()