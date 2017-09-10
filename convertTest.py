from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
# from pyspark import SparkContext
# from pyspark.sql import SQLContext
# from pyspark.sql.types import *


def toParquet():
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
        StructField("col4", StringType(), True),
        ])

    rdd = sc.textFile("/home/daria/Documents/Parquet/uber_travel_data_aug17.csv").map(lambda line: line.split(","))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('/home/daria/Documents/Parquet/uber')

def toCsv():

    sc = SparkContext(appName="Parquet2CSV")
    sqlContext = SQLContext(sc)

    readdf = sqlContext.read.parquet('/home/daria/Documents/Parquet/uber')
    readdf.rdd.map(tuple).map(
        lambda row: str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "," +
                    str(row[3])).saveAsTextFile("/home/daria/Documents/Parquet/parquet-to-csv.csv")

# toParquet()
toCsv()