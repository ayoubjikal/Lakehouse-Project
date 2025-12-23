#!/bin/bash

echo "=========================================="
echo "Real-Time Data Lake Pipeline Startup"
echo "=========================================="

# Clean up old containers and volumes (optional)
read -p "Do you want to clean up old containers and volumes? (y/N): " cleanup
if [[ $cleanup == "y" || $cleanup == "Y" ]]; then
    echo "Cleaning up..."
    docker-compose down -v
    echo "Cleanup complete."
fi

# Build and start services
echo ""
echo "Building Docker images..."
docker-compose build

echo ""
echo "Starting services..."
docker-compose up -d catalog storage mc kafka

echo ""
echo "Waiting for core services to be healthy (30 seconds)..."
sleep 30

echo ""
echo "Starting Spark and other services..."
docker-compose up -d

echo ""
echo "=========================================="
echo "Services Status:"
echo "=========================================="
docker-compose ps

echo ""
echo "=========================================="
echo "Access URLs:"
echo "=========================================="
echo "- MinIO Console: http://localhost:9001 (admin/password)"
echo "- Trino UI: http://localhost:8080"
echo "- Spark UI: http://localhost:8088"
echo "- Nessie API: http://localhost:19120"
echo "- FastAPI: http://localhost:8000"
echo ""
echo "To view logs: docker-compose logs -f [service_name]"
echo "To stop all services: docker-compose down"
echo "=========================================="