curl -X POST http://localhost:8000/order -H "Content-Type: application/json" -d '{"order_id": "123", "user_id": "u001", "items": ["apple", "banana"], "total": 42.5}'

# rebuild shared container
docker build -f shared/Dockerfile -t kare8-shared:latest ./shared

# compose

docker compose down -v # remove all containers and volumes

docker compose up --build # rebuild all container

# for k8s

 - spammer : docker run -d --name spammer -e API_URL=http://host.docker.internal:30080/order -e GENERATOR_INTERVAL=2 ghcr.io/rpsbobby/kare8-spammer:develop
- ingress proxy: helm install ingress-nginx ingress-nginx/ingress-nginx   --namespace kare8   --create-namespace   --set controller.service.type=NodePort