CLUSTER ?= kind

.PHONY: build load deploy teardown up status cluster-create cluster-delete

cluster-create:
	kind create cluster --name $(CLUSTER)

cluster-delete:
	kind delete cluster --name $(CLUSTER)

build:
	docker build -t gateway                    -f cmd/gateway/Dockerfile .
	docker build -t shippy-consignment-service -f cmd/consignment-service/Dockerfile .
	docker build -t shippy-vessel-service      -f cmd/vessel-service/Dockerfile .
	docker build -t shippy-user-service        -f cmd/user-service/Dockerfile .
	docker build -t shippy-reservation         -f cmd/reservation-service/Dockerfile .
	docker build -t shippy-payment             -f cmd/payment-service/Dockerfile .

load:
	kind load docker-image --name $(CLUSTER) gateway
	kind load docker-image --name $(CLUSTER) shippy-consignment-service
	kind load docker-image --name $(CLUSTER) shippy-vessel-service
	kind load docker-image --name $(CLUSTER) shippy-user-service
	kind load docker-image --name $(CLUSTER) shippy-reservation
	kind load docker-image --name $(CLUSTER) shippy-payment

# Apply manifests in dependency order. Wait for Kafka and Tempo before starting
# services that call EnsureTopics on boot or export traces.
deploy:
	kubectl apply -f deploy/k8s/kafka/
	kubectl apply -f deploy/k8s/observability/
	kubectl rollout status deployment/kafka --timeout=120s
	kubectl rollout status deployment/tempo --timeout=60s
	kubectl apply -f deploy/k8s/user-service/
	kubectl apply -f deploy/k8s/vessel-service/
	kubectl apply -f deploy/k8s/reservation-service/
	kubectl apply -f deploy/k8s/payment-service/
	kubectl apply -f deploy/k8s/consignment-service/
	kubectl apply -f deploy/k8s/gateway/

teardown:
	kubectl delete -f deploy/k8s/gateway/             --ignore-not-found
	kubectl delete -f deploy/k8s/consignment-service/ --ignore-not-found
	kubectl delete -f deploy/k8s/payment-service/     --ignore-not-found
	kubectl delete -f deploy/k8s/reservation-service/ --ignore-not-found
	kubectl delete -f deploy/k8s/vessel-service/      --ignore-not-found
	kubectl delete -f deploy/k8s/user-service/        --ignore-not-found
	kubectl delete -f deploy/k8s/observability/       --ignore-not-found
	kubectl delete -f deploy/k8s/kafka/               --ignore-not-found

status:
	kubectl get pods,services -o wide

forward:
	kubectl port-forward service/gateway 8080:8080

forward-grafana:
	kubectl port-forward service/grafana 3000:3000

smoke:
	@bash scripts/smoke.sh

# Build images, load into kind, and deploy everything.
up: build load deploy
