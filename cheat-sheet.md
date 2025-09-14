# v2 K8s + Helm Cheat Sheet

### 🔹 Manual Clean Install (0 → working)
1. Install ingress-nginx
```bash
helm upgrade --install ingress-nginx ingress-nginx \
--repo https://kubernetes.github.io/ingress-nginx \
--namespace ingress-nginx \
--create-namespace
```
2. Verify:
```bash
kubectl get pods -n ingress-nginx
kubectl get svc -n ingress-nginx
```

Look for ingress-nginx-controller and its NodePort (usually 80:xxxxx/TCP).

3. Install order-api
```bash
helm upgrade --install order-api ./kare8-service \
-f ./kare8-service/values/values-order-api.yaml \
-n kare8 \
--create-namespace
```

4. Install invoice-service
```bash
helm upgrade --install invoice-service ./kare8-service \
-f ./kare8-service/values/values-invoice-service.yaml \
-n kare8
```

5. Install email-service
```bash
helm upgrade --install email-service ./kare8-service \
-f ./kare8-service/values/values-email-service.yaml \
-n kare8
```
6. Install kafka-consumer
```bash
helm upgrade --install kafka-consumer ./kare8-service \
-f ./kare8-service/values/values-kafka-consumer.yaml \
-n kare8
```

✅ Verify everything
```bash
kubectl get pods,svc,ingress,servicemonitor -n kare8
kubectl get pods -n ingress-nginx
```

7. install monoitoring chart
```bash
helm upgrade --install prometheus-agent ./kare8-monitoring \
  -f ./kare8-monitoring/values/values-prometheus-agent.yaml \
  -f ./kare8-monitoring/values/values-dev.yaml \
  -n kare8 \
  --create-namespace
```

Test external access (assuming NodePort 32217):
```bash
curl -H "Host: kare8.local" http://localhost:32217/order -X POST
```

### Nginx config - How to check the values

Bridge IP:
```bash
ip addr show docker0 | grep inet
```
# → usually "172.17.0.1"


Ingress NodePort:
```bash 
kubectl get svc -n ingress-nginx ingress-nginx-controller
```
# Look under PORT(S), e.g. 80:32224/TCP

3. nginx.conf template

Edit nginx.conf (mounted into /etc/nginx/conf.d/default.conf):
```yaml
upstream kare8_ingress {
    server <bridge-ip>:<nodeport>;
}

server {
    listen 80;

    location /order {
        proxy_pass http://kare8_ingress/order;
    }
}
```



### 🔹 Manual Teardown (working → clean)
1. Uninstall services
```bash
helm uninstall order-api -n kare8
helm uninstall invoice-service -n kare8
helm uninstall email-service -n kare8
helm uninstall kafka-consumer -n kare8
```
2. Uninstall ingress-nginx and prometheus agent 
```bash
helm uninstall ingress-nginx -n ingress-nginx
helm uninstall prometheus-agent -n kare8
```

3. Delete namespaces (optional for full reset)
```bash
kubectl delete namespace kare8
kubectl delete namespace ingress-nginx
```
4. Clean cluster-scoped leftovers (IngressClass, Webhook, ClusterRoles)
```bash
kubectl delete ingressclass nginx --ignore-not-found
kubectl delete validatingwebhookconfigurations ingress-nginx-admission --ignore-not-found
kubectl delete clusterrole,clusterrolebinding -l app.kubernetes.io/instance=ingress-nginx --ignor
```

After every install check bridge ip and nodeport:
```bashkubectl get svc -n ingress-nginx
```
# v1 Compose Cheat Sheet

# Make sure .env has IMAGE_TAG=develop (or latest / sha-<commit>)
`docker compose pull`
`docker compose up -d`

# Runs GHCR images plus bind mounts so local code changes take effect immediately (optionally local builds if you uncomment them in docker-compose.dev.yml).
`docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d`
# If you want to rebuild locally for one or more services (after uncommenting build: for them):
`docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build`

1) Pin to a specific commit SHA
Edit .env:
__IMAGE_TAG=sha-f5f46bbce759c1de814b774449420e5bfb30ffc1__

`docker compose pull`
`docker compose up -d`

# This guarantees you’re running the exact build from that commit.