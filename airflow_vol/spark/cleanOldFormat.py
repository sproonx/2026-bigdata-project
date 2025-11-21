#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, round as spark_round, radians, sin, cos, atan2, sqrt, date_format
)
from pyspark.sql.types import DoubleType, IntegerType
import re
import argparse

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
    df_list = []

    try:
        stations = (
            spark.read.option('header', 'true')
            .option('inferSchema', 'true')
            .csv(stations_path)
            .select(
                col('Station ID').alias('station_id'),
                col('Latitude').alias('latitude'),
                col('Longitude').alias('longitude')
            )
        )
    except Exception as e:
        raise RuntimeError(f"Failed to read stations CSV '{stations_path}': {e}")

    for year, csv_path in csv_files:
        df = (
            spark.read.option('header', 'true')
            .option('inferSchema', 'true')
            .option('quote', '"')
            .option('escape', '\\')
            .csv(csv_path)
        )

        # Normalize/add birth year for old-format files:
        # if the dataset contains 'Birth Year' or 'birth year' use it, otherwise add a null raw column
        if 'Birth Year' in df.columns:
            df = df.withColumn('birth_year_raw', col('Birth Year').cast('string'))
        elif 'birth year' in df.columns:
            df = df.withColumn('birth_year_raw', col('birth year').cast('string'))
        else:
            df = df.withColumn('birth_year_raw', lit(None))

        # derive cleaned integer birth_year (same rules as new-format script)
        df = df.withColumn(
            'birth_year',
            when(col('birth_year_raw').isNull(), None)
            .when(col('birth_year_raw') == "\\N", None)
            .when(col('birth_year_raw') == "0", None)
            .otherwise(col('birth_year_raw').cast(IntegerType()))
        )

        df = (
            df.withColumn('trip_duration_min', spark_round(col('Duration') / 60.0, 2))
            .withColumn('trip_start_ts', to_timestamp(col('Start date'), 'M/d/yyyy H:mm'))
            .withColumn('trip_end_ts', to_timestamp(col('End date'), 'M/d/yyyy H:mm'))
            .withColumn('year_month', date_format(col('trip_start_ts'), 'yyyyMM'))
        )

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

        df = df.join(
            start_stations,
            df['Start station number'] == start_stations['start_station_id_ref'],
            how='left'
        ).join(
            end_stations,
            df['End station number'] == end_stations['end_station_id_ref'],
            how='left'
        )

        df = df.withColumn('start_latitude', col('start_latitude').cast(DoubleType()))
        df = df.withColumn('start_longitude', col('start_longitude').cast(DoubleType()))
        df = df.withColumn('end_latitude', col('end_latitude').cast(DoubleType()))
        df = df.withColumn('end_longitude', col('end_longitude').cast(DoubleType()))

        R = 6371000.0 # radius for haversine calculation

        df = df.withColumn('lat1', radians(col('start_latitude')))
        df = df.withColumn('lon1', radians(col('start_longitude')))
        df = df.withColumn('lat2', radians(col('end_latitude')))
        df = df.withColumn('lon2', radians(col('end_longitude')))

        df = df.withColumn('dlat', col('lat2') - col('lat1'))
        df = df.withColumn('dlon', col('lon2') - col('lon1'))

        df = df.withColumn(
            'a',
            sin(col('dlat') / 2) ** 2 + cos(col('lat1')) * cos(col('lat2')) * sin(col('dlon') / 2) ** 2
        ).withColumn('c', 2 * atan2(sqrt(col('a')), sqrt(1 - col('a')))).withColumn(
            'trip_distance_m', spark_round(R * col('c'), 2)
        )

        df = df.drop('lat1', 'lon1', 'lat2', 'lon2', 'dlat', 'dlon', 'a', 'c', 'start_station_id_ref', 'end_station_id_ref')

        df = df.select(
            'trip_duration_min',
            'trip_start_ts',
            'trip_end_ts',
            col('Start station number').alias('start_station_id'),
            col('Start station name').alias('start_station_name'),
            col('End station number').alias('end_station_id'),
            col('End station name').alias('end_station_name'),
            col('Bike number').alias('bike_id'),
            # use derived birth_year (will be null if absent)
            'birth_year',
            col('Gender').alias('gender'),
            'trip_distance_m',
            'year_month',
        )

        df_list.append(df)

    final_df = df_list[0]
    for d in df_list[1:]:
        final_df = final_df.unionByName(d)

    (
        final_df.write.mode('overwrite')
        .partitionBy('year_month')
        .parquet(output_path)
    )

    spark.stop()


if __name__ == '__main__':
    main()
