#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_timestamp, when, lit, round as spark_round, hour)
from pyspark.sql.types import IntegerType, DoubleType, StringType
import re
import argparse
from utilFormat import add_time_slot, calculate_haversine_distance, validate_and_clean_data, write_parquet_output

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

def normalize_birth_year(df):
    df = (
        df.withColumn("birth_year_raw", col("birth year"))
        .withColumn(
            "birth_year",
            when(col("birth_year_raw").isNull(), None)
            .when(col("birth_year_raw") == "\\N", None)
            .when(col("birth_year_raw") == "0", None)
            .otherwise(col("birth_year_raw").cast(IntegerType()))
        )
    )
    
    df = df.withColumn(
        "birth_year",
        when(col("birth_year").isNotNull() & 
             col("birth_year").between(1900, 2020), col("birth_year"))
        .otherwise(None)
    )
    
    return df

def add_temporal_columns(df, ym):
    df = (
        df.withColumn("trip_duration_min", spark_round(col("tripduration") / 60.0, 2))
        .withColumn("trip_start_ts", to_timestamp(col("starttime")))
        .withColumn("trip_end_ts", to_timestamp(col("stoptime")))
        .withColumn("year", lit(ym[:4]))
        .withColumn("month", lit(ym[4:6]))
    )
    df = add_time_slot(df, "trip_start_ts")
    return df


def process_csv_file(spark, csv_path, ym):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv(csv_path)
    )

    df = add_temporal_columns(df, ym)
    df = normalize_birth_year(df)
    df = calculate_haversine_distance(
        df,
        start_lat_col="start station latitude",
        start_lon_col="start station longitude",
        end_lat_col="end station latitude",
        end_lon_col="end station longitude",
        output_col="trip_distance_m"
    )

    df = df.withColumn(
        "gender",
        when(col("gender").isNull(), None)
        .when(col("gender") == "\\N", None)
        .otherwise(col("gender").cast(IntegerType()))
    )

    # unify column names and types
    df = df.select(
        col("trip_duration_min").cast(DoubleType()),
        col("trip_start_ts"),
        col("trip_end_ts"),
        col("start station id").cast(StringType()).alias("start_station_id"),
        col("start station name").cast(StringType()).alias("start_station_name"),
        col("end station id").cast(StringType()).alias("end_station_id"),
        col("end station name").cast(StringType()).alias("end_station_name"),
        col("bikeid").cast(StringType()).alias("bike_id"),
        col("birth_year").cast(IntegerType()),
        col("gender").cast(IntegerType()),
        col("trip_distance_m").cast(DoubleType()),
        col("time_slot").cast(StringType()),
        col("year").cast(StringType()),
        col("month").cast(StringType()),
    )
    
    df = validate_and_clean_data(df)
    return df


def main():
    parser = argparse.ArgumentParser(description="Clean Hubway new format files")
    parser.add_argument("--base-path", dest="base_path", required=True, help="Input base path (hdfs) containing yyyyMM folders")
    parser.add_argument("--output-path", dest="output_path", required=True, help="Output parquet path (hdfs) for cleaned data")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Hubway ETL - New Format")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    base_path = args.base_path
    output_path = args.output_path

    csv_files = list_valid_input_csvs(spark, base_path)
    print(f"{len(csv_files)} month folders")

    df_list = []
    for ym, csv_path in csv_files:
        print(f"Processing {ym}: {csv_path}")
        df = process_csv_file(spark, csv_path, ym)
        record_count = df.count()
        print(f"  Processed {record_count} valid records for {ym}")
        df_list.append(df)

    print("Combining all months...")
    final_df = df_list[0]
    for df in df_list[1:]:
        final_df = final_df.unionByName(df)

    total_records = final_df.count()
    print(f"Valid entries: {total_records}")

    print(f"Partitioned writing to {output_path}")
    write_parquet_output(final_df, output_path)
    spark.stop()

if __name__ == "__main__":
    main()