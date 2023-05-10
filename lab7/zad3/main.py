from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col

SOURCE_PATH = "raw_data_1.csv"
OUTPUT_PATH = "/output"
COLUMN = "Country"


def start_spark():
    sc = SparkContext('local')
    spark = SparkSession(sc)
    return spark


def data_reader(spark, filepath, filetype="csv"):
    df = spark.read.format(filetype) \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(filepath)

    return df


def data_writer(df, path):
    df.write.csv(path)


def to_upper(string):
    return string.upper()


udf_to_upper = udf(to_upper, StringType())


def data_transformer(df, column):
    df = df.withColumn(f"{column}_lowered", udf_to_upper(col(column)))
    return df


if __name__ == '__main__':
    spark = start_spark()
    df = data_reader(spark, SOURCE_PATH)
    df.show()
    df = data_transformer(df, COLUMN)
    #data_writer(df, OUTPUT_PATH)
    spark.stop()

