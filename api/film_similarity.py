from utils.spark_utils import get_spark_session
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import DoubleType
from api.data import fetch_all_films, fetch_all_films_by_genre


class FilmSimilarityFinder:
    """
    This class is used to find similar films based on the film_id.
    A simple similarity score is calculated using the jaccard similarity and year similarity.
    """

    def __init__(self):
        self.spark = get_spark_session("film_similarity_finder")
        self.all_films_df = fetch_all_films()

    def _build_similarity_udf(self, target_film_dict):
        def jaccard_similarity(list1, list2, delimiter=","):
            list1 = [x.strip() for x in list1.split(delimiter)]
            list2 = [x.strip() for x in list2.split(delimiter)]
            if not list1 or not list2:
                return 0.0
            return len(set(list1) & set(list2)) / len(set(list1) | set(list2))

        def year_similarity(y1, y2):
            # Extract year from datetime objects
            year1 = y1.year if hasattr(y1, "year") else y1
            year2 = y2.year if hasattr(y2, "year") else y2

            # Handle None values
            if year1 is None or year2 is None:
                return 0.0

            return max(0.0, 1.0 - abs(year1 - year2) / 10.0)

        def score(other):
            title_sim = jaccard_similarity(
                target_film_dict["title"], other["title"], delimiter=" "
            )
            director_sim = jaccard_similarity(
                target_film_dict["director"], other["director"]
            )
            genre_sim = jaccard_similarity(target_film_dict["genres"], other["genres"])
            cast_sim = jaccard_similarity(target_film_dict["cast"], other["cast"])
            country_sim = jaccard_similarity(
                target_film_dict["country"], other["country"]
            )
            year_sim = year_similarity(
                target_film_dict["release_year"], other["release_year"]
            )
            adult_sim = 1.0 if target_film_dict["adult"] == other["adult"] else 0.0

            score = (
                0.2 * title_sim
                + 0.2 * director_sim
                + 0.2 * genre_sim
                + 0.1 * cast_sim
                + 0.1 * country_sim
                + 0.1 * year_sim
                + 0.1 * adult_sim
            )
            return score * 100

        return udf(score, DoubleType())

    def find_similar(self, film_id, threshold=50.0):
        target_film = (
            self.all_films_df.filter(col("id") == film_id).limit(1).collect()[0]
        )
        target_dict = target_film.asDict()

        genres = [g.strip() for g in target_dict["genres"].split(",")]
        candidate_df = fetch_all_films_by_genre(genres)
        if candidate_df is None:
            return self.spark.createDataFrame([], self.all_films_df.schema)

        candidate_df = candidate_df.filter(col("id") != film_id)

        sim_udf = self._build_similarity_udf(target_dict)
        candidate_df = candidate_df.withColumn(
            "similarity", sim_udf(struct(*candidate_df.columns))
        )

        return candidate_df.filter(col("similarity") >= threshold).orderBy(
            col("similarity").desc()
        )
