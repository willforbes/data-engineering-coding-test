from utils.spark_utils import get_spark_session
import json
from pyspark.sql.functions import col, split, array, lit, arrays_overlap


def fetch_all_films():
    spark = get_spark_session("fetch_all_films")
    return spark.read.parquet("output/films")


def fetch_all_films_by_genre(genre):
    try:
        with open("output/genres/genre_list.json", "r") as f:
            genre_mapping = json.load(f)
        genre_dfs = []
        for g in genre:
            spark = get_spark_session(f"fetch_all_films_by_genre_{g}")
            clean_genre = [m["clean"] for m in genre_mapping if m["original"] == g][0]
            df = spark.read.parquet(f"output/genres/{clean_genre}.parquet")
            genre_dfs.append(df)

        if not genre_dfs:
            return None
        candidates_df = genre_dfs[0]
        for df in genre_dfs[1:]:
            candidates_df = candidates_df.union(df)

        return candidates_df.dropDuplicates(["id"])
    except Exception as e:
        print(f"Error fetching films by genre: {e}")
        return None


def filter_list_column(df, column, values):
    df = df.withColumn(f"{column}_array", split(col(column), ",\s*"))
    df = df.withColumn(f"{column}_lookup", array([lit(v) for v in values])).withColumn(
        f"{column}_match",
        arrays_overlap(col(f"{column}_array"), col(f"{column}_lookup")),
    )
    df = df[df[f"{column}_match"]]
    df = df.drop(f"{column}_array", f"{column}_lookup", f"{column}_match")
    return df
