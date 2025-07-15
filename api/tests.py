from fastapi.testclient import TestClient
import pytest
from datetime import datetime

from api.main import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client


def test_filter_films(client):
    response = client.get(
        "/films?genres=Action&genres=SciFi&adult=False&directors=Michael Hegner&countries=United States&cast=Tom Kane&release_year_min=2010&release_year_max=2020&duration_min=20&duration_max=110"
    )
    assert response.status_code == 200
    # Check that all films have the genres Action or SciFi and are not adult
    for film in response.json():
        assert "Action" in film["genres"] or "SciFi" in film["genres"]
        assert "Michael Hegner" in film["director"]
        assert "United States" in film["country"]
        assert "Tom Kane" in film["cast"]
        assert datetime.strptime(film["release_year"], "%Y-%m-%d").year >= 2010
        assert datetime.strptime(film["release_year"], "%Y-%m-%d").year <= 2020
        assert film["durationMins"] >= 20
        assert film["durationMins"] <= 110
        assert not film["adult"]


def test_get_similar_films(client):
    response = client.get("/similar_films/1")
    assert response.status_code == 200
    print(response.json())
