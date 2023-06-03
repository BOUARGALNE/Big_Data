from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("tp").master("local[*]").getOrCreate()
dfLines = spark.readStream.format("socket").option(
    "host", "localhost").option("port", 8888).load()

# Split the lines using space as delimiter
dfVentes = dfLines.select(split(dfLines["value"], " ").alias("ventes"))

# Create a new dataframe with columns dfVentes[1] and dfVentes[3]
dfVentesPair = dfVentes.select(dfVentes["ventes"][1].alias(
    "ville"), dfVentes["ventes"][3].alias("col2"))


# Group by the first column and sum the values of column 2
dfVenteCount = dfVentesPair.groupBy("ville").agg(
    sum("col2").alias("sum or prices"))

dfVenteCount.writeStream.format("console").outputMode("update").trigger(
    processingTime='3 seconds').start().awaitTermination()
