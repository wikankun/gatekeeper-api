from fastapi.testclient import TestClient
from .app import app

client = TestClient(app)


def test_index():
    response = client.get("/")
    assert response.status_code == 200

def test_create_activities_success():
    # arrange
    payload = {
        "activities": [
            {
                "operation": "insert",
                "table": "table1",
                "col_names": ["a", "b", "c"],
                "col_types": ["INTEGER", "TEXT", "TEXT"],
                "col_values": [1, "Backup and Restore", "2018-03-27 11:58:28.988414"]
            },
            {
                "operation": "delete",
                "table": "table1",
                "old_value": {
                    "col_names": ["a", "c"],
                    "col_types": ["INTEGER", "TEXT"],
                    "col_values": [3, "2019-04-28 10:24:30.183414"]
                }
            }
        ]
    }
    # act
    response = client.post(
        "/api/activities",
        json=payload,
    )
    # assert
    assert response.status_code == 200
    assert response.json() == {"message": "ok", "payload": payload}

def test_create_activities_error_incomplete_payload():
    # arrange
    payload = {
        "activities": [
            {
                "operation": "insert",
                "table": "table1",
                "col_names": ["a", "b", "c"],
                "col_types": ["INTEGER", "TEXT", "TEXT"]
            }
        ]
    }
    # act
    response = client.post(
        "/api/activities",
        json=payload,
    )
    # assert
    assert response.status_code == 406

def test_create_activities_error_not_recognized_operation():
    # arrange
    payload = {
        "activities": [
            {
                "operation": "some operation",
                "table": "table1",
                "col_names": ["a", "b", "c"],
                "col_types": ["INTEGER", "TEXT", "TEXT"],
                "col_values": [1, "Backup and Restore", "2018-03-27 11:58:28.988414"]
            }
        ]
    }
    # act
    response = client.post(
        "/api/activities",
        json=payload,
    )
    # assert
    assert response.status_code == 406
    assert 'error' in response.json()
