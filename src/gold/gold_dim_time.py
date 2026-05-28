from pyspark.sql import DataFrame #type:ignore
from pyspark.sql import functions as F #type:ignore
from pyspark.sql.functions import col, lit, when, expr, greatest, row_number
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_dim_time")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "dim_time"

    target_table = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"

    # ============================================================
    # CONFIGURATION
    # ============================================================
    start_date = "1900-01-01"
    end_date   = "2100-12-31"

    # ============================================================
    # 1) Generate date range
    # ============================================================
    df_dates = (
        spark.sql(
            f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) AS date_list"
        )
        .select(F.explode("date_list").alias("full_date"))
    )

    # ============================================================
    # 2) Time Dimension Attributes
    # ============================================================
    df_time = (
        df_dates
        .withColumn("time_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("date",            F.col("full_date"))
        .withColumn("year",            F.year("full_date"))
        .withColumn("quarter",         F.quarter("full_date"))
        .withColumn("quarter_name",    F.concat(F.lit("Q"), F.quarter("full_date")))
        .withColumn("month",           F.month("full_date"))
        .withColumn("month_name",      F.date_format("full_date", "MMMM"))
        .withColumn("month_short",     F.date_format("full_date", "MMM"))
        .withColumn("day",             F.dayofmonth("full_date"))
        .withColumn("day_name",        F.date_format("full_date", "EEEE"))
        .withColumn("iso_day_of_week", ((F.dayofweek("full_date") + 5) % 7 + 1).cast("int"))
        .withColumn("iso_week_number", F.weekofyear(F.date_sub(F.col("full_date"), 1)))
        .withColumn("week_of_year",    F.weekofyear("full_date"))
        .withColumn("is_weekend",      (F.col("iso_day_of_week") >= 6).cast("boolean"))
        .withColumn("is_month_start",  (F.dayofmonth("full_date") == 1).cast("boolean"))
        .withColumn("is_month_end",
                    (F.last_day("full_date") == F.col("full_date")).cast("boolean"))
        .withColumn("is_quarter_start",
                    F.expr("date_format(full_date, 'MM-dd') in ('01-01','04-01','07-01','10-01')").cast("boolean"))
        .withColumn("is_quarter_end",
                    F.expr("date_format(full_date, 'MM-dd') in ('03-31','06-30','09-30','12-31')").cast("boolean"))
        .withColumn("is_year_start",   (F.date_format("full_date", "MM-dd") == "01-01").cast("boolean"))
        .withColumn("is_year_end",     (F.date_format("full_date", "MM-dd") == "12-31").cast("boolean"))
    )

    # ============================================================
    # 3) Holidays
    # ============================================================
    holidays = [
        ("1900-01-01", "New Year"),
        ("1900-12-25", "Christmas"),
    ]

    df_holidays = (
        spark.createDataFrame(holidays, ["date_str", "holiday_name"])
             .withColumn("holiday_date", F.to_date("date_str"))
             .drop("date_str")
    )

    df_time = (
        df_time
        .join(df_holidays, df_time["date"] == df_holidays["holiday_date"], "left")
        .drop("holiday_date", "full_date")
        .withColumn("holiday_name", F.col("holiday_name"))
        .withColumn("is_holiday",   F.col("holiday_name").isNotNull())
    )

    # ============================================================
    # 4) Write (Iceberg)
    # ============================================================
    df_time.write.format("iceberg").mode("overwrite").saveAsTable(target_table)

    logging.info("dim_time generated successfully.")

if __name__ == "__main__":
    main()
