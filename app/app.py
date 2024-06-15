from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    month,
    desc,
    avg,
    year,
    trim,
    split,
    explode,
    sum,
)
import logging

app = Flask(__name__)

spark = (
    SparkSession.builder.appName("Python Spark SQL data source example")
    .config("spark.jars", "/opt/drivers/postgresql-42.2.5.jar")
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://container_banco:5432/spotify")
    .option("dbtable", "spotify")
    .option("user", "adm")
    .option("password", "123")
    .option("driver", "org.postgresql.Driver")
    .load()
)


@app.route("/top10-artists", methods=["GET"])
def top_ten_artists():
    if request.args.get("month"):
        try:
            target_month = int(request.args.get("month"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid or no month provided"}), 400
    else:
        target_month = None

    if request.args.get("year"):
        try:
            target_year = int(request.args.get("year"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid or no year provided"}), 400
    else:
        target_year = None

    try:
        df_filtered = df

        if target_year is not None:
            df_filtered = df_filtered.filter(year(col("dates")) == target_year)

        if target_month is not None:
            df_filtered = df_filtered.filter(month(col("dates")) == target_month)

        df_grouped = df_filtered.groupBy("ids").agg(
            avg("monthly_listeners").alias("avg_monthly_listeners")
        )

        # Média de ouvintes mensais
        df_sorted = df_grouped.orderBy(desc("avg_monthly_listeners"))

        # Selecionar os 10 artistas mais ouvidos
        top_10_artists = df_sorted.limit(10)
        top_10_artists = top_10_artists.join(
            df_filtered.select("ids", "names", "genres").distinct(), on="ids"
        )
        top_10_artists = top_10_artists.toPandas().to_dict(orient="records")

        return jsonify(top_10_artists)
    except Exception:
        logging.error()
        return jsonify({"error": "Internal error"}), 500


@app.route("/top10-genres", methods=["GET"])
def top_ten_genres():
    try:
        if request.args.get("month"):
            try:
                target_month = int(request.args.get("month"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid or no month provided"}), 400
        else:
            target_month = None

        if request.args.get("year"):
            try:
                target_year = int(request.args.get("year"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid or no year provided"}), 400
        else:
            target_year = None

        df_filtered = df

        if target_year:
            df_filtered = df_filtered.filter(year(col("dates")) == target_year)

        if target_month:
            df_filtered = df_filtered.filter(month(col("dates")) == target_month)

        # Filtrar o DataFrame
        df_filtered = df_filtered.filter(
            (col("genres").isNotNull()) & (trim(col("genres")) != "")
        )

        # Separar e explodir os gêneros
        df_split = df_filtered.withColumn("genres", split(col("genres"), ", "))
        df_exploded = df_split.withColumn("genre", explode(col("genres")))

        # Calcular a média de ouvintes mensais por artista e gênero
        df_artists_average = df_exploded.groupBy("ids", "genre").agg(
            avg("monthly_listeners").alias("avg_monthly_listeners")
        )

        # Somar as médias de ouvintes mensais por gênero
        df_genre_sum = df_artists_average.groupBy("genre").agg(
            sum("avg_monthly_listeners").alias("sum_monthly_listeners")
        )

        # Ordenar os gêneros pela média de ouvintes mensais em ordem decrescente
        df_sorted = df_genre_sum.orderBy(desc("sum_monthly_listeners"))

        # Selecionar os top 10 gêneros
        top_10_genres = df_sorted.limit(10)
        top_10_genres = top_10_genres.toPandas().to_dict(orient="records")

        return jsonify(top_10_genres)
    except Exception as e:
        logging.error(e)
        return jsonify({"error": "Internal error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)
