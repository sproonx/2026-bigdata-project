#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, count, avg, sum as spark_sum, when, lit, 
    round as spark_round, row_number, desc, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import argparse
import pandas as pd


def calculate_age_from_birth_year(year_col, month_col, birth_year_col):
    return year_col.cast(IntegerType()) - birth_year_col


def validate_input_data(df):
    required_columns = {
        'trip_duration_min', 'trip_start_ts', 'trip_end_ts',
        'start_station_id', 'start_station_name', 'end_station_id', 
        'end_station_name', 'bike_id', 'trip_distance_m', 'time_slot', 'year', 'month'
    }
    
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"Input data missing required columns: {missing_cols}")
    
    if df.count() == 0:
        raise ValueError("Input data is empty")
    
    print("Input data validation passed")
    return df


def main():
    parser = argparse.ArgumentParser(description="Calculate KPIs for bikesharing data")
    parser.add_argument("--input-path", dest="input_path", required=True, 
                        help="Input parquet path (hdfs) containing cleaned data")
    parser.add_argument("--output-path", dest="output_path", required=True, 
                        help="Output path (hdfs) for KPI results")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Bikesharing KPI Calculation")
        .getOrCreate()
    )

    input_path = args.input_path
    output_path = args.output_path

    df = spark.read.parquet(input_path)
    df = validate_input_data(df)

    df = df.withColumn("trip_distance_km", spark_round(col("trip_distance_m") / 1000.0, 2))
    
    df = df.withColumn(
        "age", 
        when(col("birth_year").isNotNull(), 
             calculate_age_from_birth_year(col("year"), col("month"), col("birth_year")))
        .otherwise(None)
    )

    df = df.withColumn("year_month", concat_ws("-", col("year"), col("month")))

    df.cache()

    print(f"Writing KPIs to {output_path}")
    
    years = df.select("year").distinct().orderBy("year").rdd.flatMap(lambda x: x).collect()
    
    if not years:
        print("Warning: No data found in input")
        spark.stop()
        return
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for year in years:
            print(f"Processing KPIs for year {year}")
            
            year_df = df.filter(col("year") == year)
            
            months = (
                year_df.select("month")
                .distinct()
                .orderBy("month")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            
            kpi_rows = []
            
            for month in months:
                month_df = year_df.filter(col("month") == month)
                year_month = f"{year}-{month}"
                
                row_data = {"Year": year, "Month": month}
                
                avg_dur = month_df.agg(avg(col("trip_duration_min"))).collect()[0][0]
                row_data["Avg Trip Duration (min)"] = round(avg_dur, 2) if avg_dur else None
                
                avg_dist = month_df.agg(avg(col("trip_distance_km"))).collect()[0][0]
                row_data["Avg Trip Distance (km)"] = round(avg_dist, 2) if avg_dist else None
                
                gender_data = (
                    month_df.filter(col("gender").isNotNull())
                    .groupBy("gender")
                    .agg(count("*").alias("count"))
                    .collect()
                )
                total_gender = sum([r["count"] for r in gender_data])
                for g_row in gender_data:
                    gender = g_row["gender"]
                    pct = round((g_row["count"] / total_gender) * 100, 2) if total_gender > 0 else 0
                    row_data[f"Gender {gender} (%)"] = pct
                
                age_data = (
                    month_df.filter(col("age").isNotNull())
                    .filter((col("age") >= 10) & (col("age") <= 100))
                    .withColumn(
                        "age_group",
                        when(col("age") < 20, "10-19")
                        .when(col("age") < 30, "20-29")
                        .when(col("age") < 40, "30-39")
                        .when(col("age") < 50, "40-49")
                        .when(col("age") < 60, "50-59")
                        .when(col("age") < 70, "60-69")
                        .otherwise("70+")
                    )
                    .groupBy("age_group")
                    .agg(count("*").alias("count"))
                    .collect()
                )
                total_age = sum([r["count"] for r in age_data])
                for a_row in age_data:
                    age_group = a_row["age_group"]
                    pct = round((a_row["count"] / total_age) * 100, 2) if total_age > 0 else 0
                    row_data[f"Age {age_group} (%)"] = pct
                
                top_bikes_data = (
                    month_df.groupBy("bike_id")
                    .agg(count("*").alias("count"))
                    .orderBy(desc("count"))
                    .limit(10)
                    .collect()
                )
                for i, bike_row in enumerate(top_bikes_data, 1):
                    row_data[f"Top Bike {i}"] = f"{bike_row['bike_id']} ({bike_row['count']})"
                
                top_start_data = (
                    month_df.groupBy("start_station_name")
                    .agg(count("*").alias("count"))
                    .orderBy(desc("count"))
                    .limit(10)
                    .collect()
                )
                for i, station_row in enumerate(top_start_data, 1):
                    row_data[f"Top Start Station {i}"] = f"{station_row['start_station_name']} ({station_row['count']})"
                
                top_end_data = (
                    month_df.groupBy("end_station_name")
                    .agg(count("*").alias("count"))
                    .orderBy(desc("count"))
                    .limit(10)
                    .collect()
                )
                for i, station_row in enumerate(top_end_data, 1):
                    row_data[f"Top End Station {i}"] = f"{station_row['end_station_name']} ({station_row['count']})"
                
                timeslot_data = (
                    month_df.groupBy("time_slot")
                    .agg(count("*").alias("count"))
                    .collect()
                )
                total_timeslot = sum([r["count"] for r in timeslot_data])
                for ts_row in timeslot_data:
                    time_slot = ts_row["time_slot"]
                    pct = round((ts_row["count"] / total_timeslot) * 100, 2) if total_timeslot > 0 else 0
                    row_data[f"Timeslot {time_slot} (%)"] = pct
                
                kpi_rows.append(row_data)
            
            # write months to year sheet
            if kpi_rows:
                year_pdf = pd.DataFrame(kpi_rows)
                sheet_name = str(year)
                year_pdf.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Written {len(kpi_rows)} months to sheet '{sheet_name}'")

    df.unpersist()

    print("KPI calculation completed successfully!")
    print(f"Excel file saved to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()