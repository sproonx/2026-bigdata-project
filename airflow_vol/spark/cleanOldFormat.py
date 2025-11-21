#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_timestamp, when, lit, round as spark_round, date_format, hour, sum as spark_sum)
from pyspark.sql.types import DoubleType, IntegerType, StringType
import re
import argparse
from utilFormat import add_time_slot, calculate_haversine_distance, validate_and_clean_data, write_parquet_output

FOLDER_PATTERN = re.compile(r"^[0-9]{4}$")
STATIONS_DEFAULT_NAME = "Hubway_Stations_2011_2016.csv"
CSV_FILE_NAME = "data.csv"

def list_valid_input_csvs_old(spark, base_path):
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
        raise RuntimeError(f"No year folders with CSVs found under '{base_path}'")
    return sorted(paths, key=lambda x: x[0])


def read_stations_data(spark, stations_path):
    try:
        stations = (
            spark.read.option('header', 'true')
            .option('inferSchema', 'true')
            .csv(stations_path)
            .select(
                col('Station ID').cast(StringType()).alias('station_id'),
                col('Latitude').cast(DoubleType()).alias('latitude'),
                col('Longitude').cast(DoubleType()).alias('longitude')
            )
        )
        stations = stations.filter(
            col('station_id').isNotNull() &
            col('latitude').isNotNull() &
            col('longitude').isNotNull() &
            (col('latitude').between(-90, 90)) &
            (col('longitude').between(-180, 180))
        )
        
        return stations
    except Exception as e:
        raise RuntimeError(f"Failed to read stations CSV '{stations_path}': {e}")


def add_temporal_columns(df):
    df = (
        df.withColumn('trip_duration_min', spark_round(col('Duration') / 60000.0, 2))  # ms -> minutes
        .withColumn('trip_start_ts', to_timestamp(col('Start date'), 'M/d/yyyy H:mm'))
        .withColumn('trip_end_ts', to_timestamp(col('End date'), 'M/d/yyyy H:mm'))
        .withColumn('year', date_format(col('trip_start_ts'), 'yyyy'))
        .withColumn('month', date_format(col('trip_start_ts'), 'MM'))
    )
    df = add_time_slot(df, 'trip_start_ts')
    df = df.withColumn('birth_year', lit(None).cast(IntegerType()))
    return df


def join_station_data(df, stations):
    start_stations = stations.select(
        col('station_id').alias('start_station_id_ref'),
        col('latitude').alias('start_latitude'),
        col('longitude').alias('start_longitude')
    )

    end_stations = stations.select(
        col('station_id').alias('end_station_id_ref'),
        col('latitude').alias('end_latitude'),
        col('longitude').alias('end_longitude')
    )

    df = (
        df.join(
            start_stations,
            df['Start station number'] == start_stations['start_station_id_ref'],
            how='left'
        ).join(
            end_stations,
            df['End station number'] == end_stations['end_station_id_ref'],
            how='left'
        )
        .withColumn('start_latitude', col('start_latitude').cast(DoubleType()))
        .withColumn('start_longitude', col('start_longitude').cast(DoubleType()))
        .withColumn('end_latitude', col('end_latitude').cast(DoubleType()))
        .withColumn('end_longitude', col('end_longitude').cast(DoubleType()))
        .drop('start_station_id_ref', 'end_station_id_ref')
    )
    
    return df

def process_csv_file(spark, csv_path, stations):
    df = (
        spark.read.option('header', 'true')
        .option('inferSchema', 'true')
        .option('quote', '"')
        .option('escape', '\\')
        .csv(csv_path)
    )
    
    df = add_temporal_columns(df)
    df = join_station_data(df, stations)
    df = calculate_haversine_distance(
        df,
        start_lat_col='start_latitude',
        start_lon_col='start_longitude',
        end_lat_col='end_latitude',
        end_lon_col='end_longitude',
        output_col='trip_distance_m'
    )

    df = df.withColumn(
        'gender',
        when(col('Gender').isNull(), None)
        .when(col('Gender') == "\\N", None)
        .otherwise(col('Gender').cast(IntegerType()))
    )

    # unify column names and types
    df = df.select(
        col('trip_duration_min').cast(DoubleType()),
        col('trip_start_ts'),
        col('trip_end_ts'),
        col('Start station number').cast(StringType()).alias('start_station_id'),
        col('Start station name').cast(StringType()).alias('start_station_name'),
        col('End station number').cast(StringType()).alias('end_station_id'),
        col('End station name').cast(StringType()).alias('end_station_name'),
        col('Bike number').cast(StringType()).alias('bike_id'),
        col('birth_year').cast(IntegerType()),
        col('gender').cast(IntegerType()),
        col('trip_distance_m').cast(DoubleType()),
        col('time_slot').cast(StringType()),
        col('year').cast(StringType()),
        col('month').cast(StringType()),
    )
    print(f"  Total records before cleaning: {df.count()}")

    df = validate_and_clean_data(df)
    return df


def main():
    parser = argparse.ArgumentParser(description="Clean Hubway old format files and join stations")
    parser.add_argument("--base-path", dest="base_path", required=True, help="Input base path (hdfs) containing yyyy folders")
    parser.add_argument("--stations-path", dest="stations_path", required=False, help="Path to stations CSV (hdfs). If omitted this script will look for Hubway_Stations_2011_2016.csv under base path.")
    parser.add_argument("--output-path", dest="output_path", required=True, help="Output parquet path (hdfs) for cleaned data")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Hubway ETL - Old Format")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    base_path = args.base_path
    output_path = args.output_path
    stations_path = args.stations_path

    csv_files = list_valid_input_csvs_old(spark, base_path)
    print(f"{len(csv_files)} year folders")

    stations = read_stations_data(spark, stations_path)
    stations_count = stations.count()
    print(f"{stations_count} valid stations")

    df_list = []
    for year, csv_path in csv_files:
        print(f"Processing year {year}: {csv_path}")
        df = process_csv_file(spark, csv_path, stations)
        record_count = df.count()
        print(f"  Processed {record_count} valid records for year {year}")
        df_list.append(df)

    print("Combining all years...")
    final_df = df_list[0]
    for d in df_list[1:]:
        final_df = final_df.unionByName(d)

    total_records = final_df.count()
    print(f"Valid entries: {total_records}")

    print(f"Partitioned writing to {output_path}")
    write_parquet_output(final_df, output_path)
    spark.stop()


if __name__ == '__main__':
    main()
