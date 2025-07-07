from fastapi.testclient import TestClient
import pytest

from api.main import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client


def test_filter_films(client):
    response = client.get("/films?genres=Action&genres=SciFi&adult=False")
    assert response.status_code == 200
    # Check that all films have the genres Action or SciFi and are not adult
    for film in response.json():
        assert "Action" in film["genres"] or "SciFi" in film["genres"]
        assert not film["adult"]


def test_get_similar_films(client):
    response = client.get("/similar_films/1")
    assert response.status_code == 200
    print(response.json())
