from fastapi import FastAPI, Query
from typing import List, Optional
from api.data import fetch_all_films, filter_list_column
from api.film_similarity import FilmSimilarityFinder
from datetime import datetime

app = FastAPI()


@app.get("/films")
def filter_films(
    directors: Optional[List[str]] = Query(None),
    countries: Optional[List[str]] = Query(None),
    genres: Optional[List[str]] = Query(None),
    cast: Optional[List[str]] = Query(None),
    adult: Optional[bool] = None,
    release_year_min: Optional[int] = None,
    release_year_max: Optional[int] = None,
    duration_min: Optional[int] = None,
    duration_max: Optional[int] = None,
):
    filtered_df = fetch_all_films()

    if directors:
        filtered_df = filter_list_column(filtered_df, "director", directors)

    if countries:
        filtered_df = filter_list_column(filtered_df, "country", countries)

    if genres:
        filtered_df = filter_list_column(filtered_df, "genres", genres)

    if cast:
        filtered_df = filter_list_column(filtered_df, "cast", cast)

    if adult is not None:
        filtered_df = filtered_df[filtered_df["adult"] == adult]

    if release_year_min is not None:
        filtered_df = filtered_df[
            filtered_df["release_year"] >= datetime(release_year_min, 1, 1)
        ]
    if release_year_max is not None:
        filtered_df = filtered_df[
            filtered_df["release_year"] <= datetime(release_year_max, 1, 1)
        ]

    if duration_min is not None:
        filtered_df = filtered_df[filtered_df["durationMins"] >= duration_min]
    if duration_max is not None:
        filtered_df = filtered_df[filtered_df["durationMins"] <= duration_max]

    return filtered_df.toPandas().to_dict(orient="records")


@app.get("/similar_films/{film_id}")
def get_similar_films(film_id: str, threshold: float = 50.0):
    finder = FilmSimilarityFinder()
    similar_films = finder.find_similar(film_id, threshold)
    return similar_films.toPandas().to_dict(orient="records")
