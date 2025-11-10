from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("hello_spark_job").getOrCreate()

    data = [("Madalena", 1), ("Vitor", 2), ("Beatriz", 3)]
    df = spark.createDataFrame(data, ["name", "value"])

    print("### Hello from Spark on Kubernetes via Airflow ###")
    df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()