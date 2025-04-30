from pyspark.sql import SparkSession

# Example Spark job

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("ExampleSparkJob") \
        .getOrCreate()

    # Create a DataFrame
    data = [
        (1, "John Doe", 21),
        (2, "Jane Doe", 22),
        (3, "Joe Bloggs", 23),
    ]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    df.show()

    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    main()