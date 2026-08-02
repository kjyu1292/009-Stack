#!/bin/bash
# add_dags.sh
# Copies all DAG .py files from the local airflow/dags/ folder into the
# Airflow scheduler and dag-processor pods, then checks for import errors.
#
# Usage:
#   ./add_dags.sh                  # copies every .py file in airflow/dags/
#   ./add_dags.sh my_dag.py        # copies just one specific file

set -e  # stop on first error

DAGS_DIR="$HOME/projects/009-stack/airflow/dags"
NAMESPACE="default"

# Find the current scheduler and dag-processor pod names
SCHEDULER_POD=$(kubectl get pods -n "$NAMESPACE" -o name | grep airflow-scheduler | cut -d/ -f2)
DAG_PROCESSOR_POD=$(kubectl get pods -n "$NAMESPACE" -o name | grep airflow-dag-processor | cut -d/ -f2)

if [ -z "$SCHEDULER_POD" ] || [ -z "$DAG_PROCESSOR_POD" ]; then
    echo "Could not find scheduler or dag-processor pod. Is Airflow running?"
    kubectl get pods -n "$NAMESPACE"
    exit 1
fi

echo "Scheduler pod:     $SCHEDULER_POD"
echo "DAG processor pod: $DAG_PROCESSOR_POD"
echo ""

# Decide which files to copy: either one named file, or everything in the folder
if [ -n "$1" ]; then
    echo "Clearing old DAG files from both pods..."
    kubectl exec "$SCHEDULER_POD" -n "$NAMESPACE" -- sh -c "rm -rf /opt/airflow/dags/*.py /opt/airflow/dags/__pycache__"
    kubectl exec "$DAG_PROCESSOR_POD" -n "$NAMESPACE" -- sh -c "rm -rf /opt/airflow/dags/*.py /opt/airflow/dags/__pycache__"
    echo ""
else
    FILES=$(ls "$DAGS_DIR"/*.py)
fi

for f in $FILES; do
    filename=$(basename "$f")
    filepath="$DAGS_DIR/$filename"

    if [ ! -f "$filepath" ]; then
        echo "Skipping $filename — not found in $DAGS_DIR"
        continue
    fi

    echo "Copying $filename ..."
    kubectl cp "$filepath" "$NAMESPACE/$SCHEDULER_POD:/opt/airflow/dags/$filename"
    kubectl cp "$filepath" "$NAMESPACE/$DAG_PROCESSOR_POD:/opt/airflow/dags/$filename"
done

echo ""
echo "Forcing a synchronous reserialize (registers all DAGs immediately)..."
kubectl exec "$DAG_PROCESSOR_POD" -n "$NAMESPACE" -- airflow dags reserialize

echo ""
echo "Waiting 15s for the dag-processor to pick up changes..."
sleep 15

echo ""
echo "Checking for import errors:"
kubectl exec -it "$SCHEDULER_POD" -n "$NAMESPACE" -- airflow dags list-import-errors

echo ""
echo "Current DAGs:"
kubectl exec -it "$SCHEDULER_POD" -n "$NAMESPACE" -- airflow dags list
