from pyspark.sql.functions import (col, hour, when, radians, sin, cos, atan2, sqrt, round as spark_round)

EARTH_RADIUS_M = 6371000.0  # meters for haversine formula

def add_time_slot(df, ts_col="trip_start_ts"):
    df = df.withColumn("trip_hour", hour(col(ts_col)))
    df = df.withColumn(
        "time_slot",
        when((col("trip_hour") >= 0) & (col("trip_hour") < 6), "00:00-06:00")
        .when((col("trip_hour") >= 6) & (col("trip_hour") < 12), "06:00-12:00")
        .when((col("trip_hour") >= 12) & (col("trip_hour") < 18), "12:00-18:00")
        .when((col("trip_hour") >= 18) & (col("trip_hour") < 24), "18:00-24:00")
        .otherwise("Unknown")
    ).drop("trip_hour")
    return df

def calculate_haversine_distance(df,start_lat_col, start_lon_col, end_lat_col, end_lon_col, output_col="trip_distance_m"):
    df = (
        df.withColumn("lat1", radians(col(start_lat_col)))
          .withColumn("lon1", radians(col(start_lon_col)))
          .withColumn("lat2", radians(col(end_lat_col)))
          .withColumn("lon2", radians(col(end_lon_col)))
          .withColumn("dlat", col("lat2") - col("lat1"))
          .withColumn("dlon", col("lon2") - col("lon1"))
          .withColumn(
              "a",
              sin(col("dlat") / 2) ** 2 +
              cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon") / 2) ** 2
          )
          .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))
          .withColumn(output_col, spark_round(col("c") * EARTH_RADIUS_M, 2))
          .drop("lat1", "lon1", "lat2", "lon2", "dlat", "dlon", "a", "c")
    )
    return df

def validate_and_clean_data(df):
    return df.filter(
        col("trip_start_ts").isNotNull() &
        col("trip_end_ts").isNotNull() &
        (col("trip_end_ts") > col("trip_start_ts")) &
        col("time_slot").isNotNull() &
        col("trip_duration_min").between(1, 1440) &
        col("trip_distance_m").between(0, 50000) &
        col("start_station_id").isNotNull() &
        col("end_station_id").isNotNull()
    )

def write_parquet_output(df, output_path):
    (df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path))
