from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, desc, sum, year

app = Flask(__name__)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL data source example") \
    .config("spark.jars", "/opt/drivers/postgresql-42.2.5.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://container_banco:5432/spotify") \
    .option("dbtable", "spotify") \
    .option("user", "adm") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Endpoint para 'month'
@app.route('/month', methods=['GET'])
def get_month():
    target_month = int(request.args.get('month'))

    if target_month is None:
        return jsonify({"error": "No month provided"}), 400
    
    # Filtrar pelo mês especificado
    df_filtered = df.filter(month(col("dates")) == target_month)
    
    # Classificar os artistas pelo número de ouvintes mensais em ordem decrescente
    df_sorted = df_filtered.orderBy(desc("monthly_listeners"))
    
    # Selecionar os 10 artistas mais ouvidos
    top_10_artists = df_sorted.select("names", "dates", "monthly_listeners").distinct().limit(10)

    top_10_artists = top_10_artists.toPandas().to_dict(orient="records")

    return jsonify(top_10_artists)

        
    
# Endpoint para 'year'
@app.route('/year', methods=['GET'])
def get_year():
    target_year = request.args.get('year')

    if target_year is None:
        return jsonify({"error": "No year provided"}), 400
    
    df_filtred = df.filter(year(col("dates")) == target_year)

    df_grouped = df_filtred.groupBy("names").agg(sum("monthly_listeners").alias("total_listeners"))
    
    # Classificar os artistas pelo total de ouvintes mensais em ordem decrescente
    df_sorted = df_grouped.orderBy(desc("total_listeners"))
    
    # Selecionar os 10 artistas mais ouvidos
    top_10_artists = df_sorted.limit(10)
    top_10_artists = top_10_artists.toPandas().to_dict(orient="records")
    
    return jsonify(top_10_artists)
    
        
    
# Endpoint para 'genre'
@app.route('/genre', methods=['GET'])
def get_genre():
    genre = request.args.get('genre')

    if genre is None:
        return jsonify({"error": "No genre provided"}), 400

    df_filtred = df.filter(col("genres").contains(genre))

    df_sorted = df_filtred.orderBy("names")

    df_sorted = df_sorted.toPandas().to_dict(orient="records")

    return jsonify(df_sorted)

        


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)