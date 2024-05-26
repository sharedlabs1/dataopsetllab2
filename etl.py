from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL_with_PySpark").getOrCreate()

    # Load data from CSV file
    df = spark.read.csv('/home/jovyan/work/sample_data.csv', header=True, inferSchema=True)

    # Transform data: Convert name to uppercase and filter age > 30
    transformed_df = df.withColumn('name', upper(col('name'))).filter(col('age') > 30)

    # Save the transformed data to a new CSV file
    transformed_df.write.csv('/home/jovyan/work/transformed_data.csv', header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
