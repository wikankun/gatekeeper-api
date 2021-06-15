from locust import HttpUser, between, task
from random import choice


class ApiUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def activities(self):
        payload_insert = {
            "activities": [
                {
                    "operation": "insert",
                    "table": "table1",
                    "col_names": ["a", "b", "c"],
                    "col_types": ["INTEGER", "TEXT", "TEXT"],
                    "col_values": [1, "Backup and Restore", "2018-03-27 11:58:28.988414"]
                }
            ]
        }
        payload_delete = {
            "activities": [
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
        self.client.post("/api/activities", json=choice([payload_insert, payload_delete]))

        