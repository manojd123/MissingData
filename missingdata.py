from pyspark.sql import SparkSession
from pyspark.sql.types   import StructType,StructField,StringType,IntegerType
from pyspark.sql import functions as psf

spark  = SparkSession.builder.appName("missing data ").master("local[*]").getOrCreate()
sc = spark.sparkContext

# Defining Schema
retail_schema = StructType([
    StructField('Sales_rep',StringType(), True),
    StructField('Region',StringType(), True),
    StructField('Month',StringType(), True),
    StructField('Sales',IntegerType(), True)

])


df = spark.read\
    .schema(retail_schema)\
    .option("delimeter","|")\
    .csv("/home/manoj/Downloads/datasets/retail_data.csv") \

df.show(10, False)

validData = df.filter(psf.col("Region").isNotNull() |  psf.col("Month").isNotNull())
invalidData= df.filter(psf.col("Region").isNull() | psf.col("Month").isNull())\
    .withColumn("Hold_reason", psf
                .when(psf.col("Region").isNull(), "Region is missing")
                .otherwise(psf.when(psf.col("Month").isNull(), "Month is missing")))

validData.write\
    .mode("overwrite")\
    .option("delimeter",",")\
    .csv("/home/manoj/Downloads/datasets/valid")

invalidData.write\
    .mode("append")\
    .option("delimeter",",")\
    .csv("/home/manoj/Downloads/datasets/invalid")
