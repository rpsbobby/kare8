#!/usr/bin/env bash
set -euo pipefail

BRIDGE_IP=$(ip -4 addr show docker0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
NODE_PORT=$(kubectl get svc -n ingress-nginx ingress-nginx-controller \
  -o jsonpath='{.spec.ports[?(@.port==80)].nodePort}')

cat > ./infra/default.conf <<EOF
upstream kare8_ingress {
    server ${BRIDGE_IP}:${NODE_PORT};
}

server {
    listen 80;

    location /order {
        proxy_pass http://kare8_ingress/order;
    }
}
EOF

echo "✅ nginx.conf regenerated"
echo "   Bridge IP : $BRIDGE_IP"
echo "   NodePort  : $NODE_PORT"
