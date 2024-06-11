# teste_conexao.py

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL data source example") \
    .config("spark.jars", "/opt/drivers/postgresql-42.2.5.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/spotify") \
    .option("dbtable", "spotify") \
    .option("user", "adm") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()

df.show()
