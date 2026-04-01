TODO

- mount sql files into postgres to create tables


SERVICE_ADDRESS=localhost:50051 go run . --name "Test" --email "test@test.com" --password "password" --company "Test Co"

SERVICE_ADDRESS=localhost:50051 go run . ./consignment.json eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjp7ImlkIjoiMGZlNDgxYzgtMmEyOC00YWQ2LTliNWUtN2RjNGQ2NzYyNWI1IiwibmFtZSI6IlRlc3QiLCJjb21wYW55IjoiVGVzdCBDbyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSIsInBhc3N3b3JkIjoiJDJhJDEwJGwwNjg1L0FWVTBnT3JMWFN3aEVueU9nLkdTV0cvTi9XcmxaRkFhUnJFbmh3N3RucnlhU0pXIn0sImV4cCI6MTc3NDk3MDE4NCwiaXNzIjoic2hpcHBpbmcuVXNlclNlcnZpY2UifQ.EEOtYmxD5xmN2xBiCY3rqx96dDOywqIlY9HKzEsxQBg


kubectl scale statefulset user-service-database --replicas=0
kubectl delete pvc user-db-data-user-service-database-0
kubectl scale statefulset user-service-database --replicas=1


kubectl port-forward svc/user-service 50051:50051

kind load docker-image shippy-consignment-service:latest         


# 1. Create a user
curl -X POST http://localhost:8080/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "company": "Shippy Inc",
    "password": "secret123"
  }'

# 2. Get a token
curl -X POST http://localhost:8080/auth \
  -H "Content-Type: application/json" \
  -d '{
    "email": "john@example.com",
    "password": "secret123"
  }'

# 3. Create a consignment (replace TOKEN with the value from step 2)
curl -X POST http://localhost:8080/v1/consignments \
  -H "Content-Type: application/json" \
  -H "x-token: TOKEN" \
  -d '{
    "description": "Laptop shipment",
    "weight": 500,
    "containers": [
      {
        "customer_id": "cust-001",
        "origin": "London",
        "user_id": "user-001"
      }
    ]
  }'
