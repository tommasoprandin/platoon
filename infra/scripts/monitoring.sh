#!/bin/bash
set -e

# Install Docker
apt-get update
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/download/v2.20.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Create monitoring directory
mkdir -p /opt/monitoring

# Create docker-compose.yml
cat > /opt/monitoring/docker-compose.yml <<EOF
version: '3'

services:
  loki:
    image: grafana/loki:3.4.1
    ports:
      - "3100:3100"
    volumes:
      - loki-data:/loki
    restart: always

  grafana:
    image: grafana/grafana:8.2.1
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - grafana-data:/var/lib/grafana
      - ./provisioning:/etc/grafana/provisioning
    depends_on:
      - loki
    restart: always

volumes:
  loki-data:
  grafana-data:
EOF

# Create Grafana provisioning directories
mkdir -p /opt/monitoring/provisioning/{datasources,dashboards}

# Start the monitoring stack
cd /opt/monitoring
docker-compose up -d

echo "Monitoring setup complete."
