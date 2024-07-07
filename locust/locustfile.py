import time
import json
from locust import HttpUser, task, between

class JsonLoadTestUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def send_json(self):
        url = "/track"
        timestamp = int(time.time())
        json_data = {
            "url": "https://example.com",
            "timestamp": timestamp
        }
        headers = {'Content-Type': 'application/json'}
        
        self.client.post(url, data=json.dumps(json_data), headers=headers)

if __name__ == "__main__":
    import os
    os.system("locust -f locustfile.py")
