#!/bin/bash

set -e
DAGS_DIR="$HOME/projects/009-stack/airflow/dags"
CLUSTER_NAME="kame"
SCHEDULER_POD=$(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2)

# Rebuild the image (--no-cache ensures the new file content is actually picked up)
docker build --no-cache -t upbase/airflow:local -f docker/airflow.Dockerfile .

# Load the fresh image into the kind cluster
kind load docker-image upbase/airflow:local --name $CLUSTER_NAME

# Restart every Airflow component so they all pick up the new image
kubectl rollout restart deployment/airflow-scheduler
kubectl rollout restart deployment/airflow-dag-processor
kubectl rollout restart statefulset/airflow-triggerer
kubectl rollout restart deployment/airflow-api-server

echo ""
echo "Sleep 15s to wait for rollout..."
sleep 15

# Confirm the file is actually in the fresh image with the right content
echo "Checking dag files..."
FILES=$(ls "$DAGS_DIR"/*.py)
for f in $FILES; do
	filename=$(basename "$f")
	filepath="$DAGS_DIR/$filename"

	if docker run --rm --entrypoint test upbase/airflow:local \
	    -f "/opt/airflow/dags/$filename"
	then
	    echo "✓ $filename"
	else
	    echo "✗ $filename"
	fi
done

# Confirm no import errors
kubectl exec -it "$SCHEDULER_POD" -- airflow dags list-import-errors
