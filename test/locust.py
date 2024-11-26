from locust import HttpUser, task, between

class FastAPIUser(HttpUser):
    wait_time = between(1, 3) 
    total_requests = 5
    
    @task
    def post_event(self):
        headers = {"Content-Type": "application/json"}
        payload = {
            "message": {
                "attributes": {
                    "bucketId": "ingesta-pruebas-cofers-domingo",
                    "eventTime": "2024-11-25T18:17:50.396079Z",
                    "eventType": "OBJECT_FINALIZE",
                    "notificationConfig": "projects/_/buckets/ingesta-pruebas-domingo/notificationConfigs/1",
                    "objectGeneration": "1732558670348402",
                    "objectId": "year=2024/month=11/day=24/company_id=c60ab568-9511-4838-b093-442e3a5f8d14/test1.avro",
                    "payloadFormat": "JSON_API_V1"
                },
                "data": "ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0IiwKICAiaWQiOiAiaW5nZXN0YS1wcnVlYmFzLWNvZmVycy1kb21pbmdvL3llYXI9MjAyNC9tb250aD0xMS9kYXk9MjQvY29tcGFueV9pZD1jNjBhYjU2OC05NTExLTQ4MzgtYjA5My00NDJlM2E1ZjhkMTQvdGVzdDEuYXZyby8xNzMyNTU4NjcwMzQ4NDAyIiwKICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL2luZ2VzdGEtcHJ1ZWJhcy1jb2ZlcnMtZG9taW5nby9vL3llYXI9MjAyNCUyRm1vbnRoPTExJTJGZGF5PTI0JTJGY29tcGFueV9pZD1jNjBhYjU2OC05NTExLTQ4MzgtYjA5My00NDJlM2E1ZjhkMTQlMkZ0ZXN0MS5hdnJvIiwKICAibmFtZSI6ICJ5ZWFyPTIwMjQvbW9udGg9MTEvZGF5PTI0L2NvbXBhbnlfaWQ9YzYwYWI1NjgtOTUxMS00ODM4LWIwOTMtNDQyZTNhNWY4ZDE0L3Rlc3QxLmF2cm8iLAogICJidWNrZXQiOiAiaW5nZXN0YS1wcnVlYmFzLWNvZmVycy1kb21pbmdvIiwKICAiZ2VuZXJhdGlvbiI6ICIxNzMyNTU4NjcwMzQ4NDAyIiwKICAibWV0YWdlbmVyYXRpb24iOiAiMSIsCiAgImNvbnRlbnRUeXBlIjogImFwcGxpY2F0aW9uL29jdGV0LXN0cmVhbSIsCiAgInRpbWVDcmVhdGVkIjogIjIwMjQtMTEtMjVUMTg6MTc6NTAuMzk2WiIsCiAgInVwZGF0ZWQiOiAiMjAyNC0xMS0yNVQxODoxNzo1MC4zOTZaIiwKICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwKICAidGltZVN0b3JhZ2VDbGFzc1VwZGF0ZWQiOiAiMjAyNC0xMS0yNVQxODoxNzo1MC4zOTZaIiwKICAic2l6ZSI6ICIzMTUxIiwKICAibWQ1SGFzaCI6ICIzcStuZ3Rqd3ZLMTUzOXZKMjVGeUJ3PT0iLAogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2Rvd25sb2FkL3N0b3JhZ2UvdjEvYi9pbmdlc3RhLXBydWViYXMtY29mZXJzLWRvbWluZ28vby95ZWFyPTIwMjQlMkZtb250aD0xMSUyRmRheT0yNCUyRmNvbXBhbnlfaWQ9YzYwYWI1NjgtOTUxMS00ODM4LWIwOTMtNDQyZTNhNWY4ZDE0JTJGdGVzdDEuYXZybz9nZW5lcmF0aW9uPTE3MzI1NTg2NzAzNDg0MDImYWx0PW1lZGlhIiwKICAiY3JjMzJjIjogIjRwSXJzUT09IiwKICAiZXRhZyI6ICJDUEtBNVlDTStJa0RFQUU9Igp9Cg==",
                "messageId": "12748230761759261",
                "publishTime": "2024-11-25T18:17:50.548Z"
            },
            "subscription": "projects/production-400914/subscriptions/ingest-transactions-event-sub"
        }
        self.client.post("/", headers=headers, json=payload)
