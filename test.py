from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLiteApp").getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:/opt/bitnami/spark/database.db") \
    .option("dbtable", "my_table") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# Các truy vấn SQL hoặc thao tác trên DataFrame df
df.show()
