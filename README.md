# UpBase Data Platform

A self-hosted, Kubernetes-native data platform built from scratch: **ClickHouse** (data warehouse), **Apache Airflow** (orchestration), **Apache Spark** (ETL), and **Apache Superset** (BI/visualization) — all containerized and deployed on Kubernetes.

Built as a hands-on learning project to understand real-world data infrastructure: container orchestration, RBAC, custom Docker images, persistent storage, and end-to-end ETL pipelines — including the debugging that comes with all of it.

---

## Demo

*https://drive.proton.me/urls/51VV1XSVZ0#GFWzgnGEZmPU*

---

## Architecture

```
                    Kubernetes cluster (kind)
┌───────────────────────────────────────────────────────────┐
│                                                           │
│   ┌──────────┐  triggers   ┌──────────┐                   │
│   │ Airflow  │ ──────────▶ │  Spark   │                   │
│   │(schedule)│             │(ETL job) │                   │
│   └──────────┘             └────┬─────┘                   │
│                                 │ read + write            │
│                                 ▼                         │
│                            ┌───────────┐    ┌───────────┐ │
│                            │ClickHouse │◀── │ Superset  │ │
│                            │(warehouse)│    │(dashboard)│ │
│                            └───────────┘    └───────────┘ │
│                                                           │
│   Supporting infra: custom Docker images (dependency      │
│   pinning), Kubernetes RBAC, PersistentVolumeClaims       │
└───────────────────────────────────────────────────────────┘
```

## Stack

| Component | Role |
|---|---|
| **Kubernetes** (kind) | Container orchestration, local dev cluster |
| **ClickHouse** | Columnar OLAP data warehouse, PVC-backed |
| **Apache Airflow** | DAG-based ETL scheduling (KubernetesExecutor), Helm-deployed |
| **Apache Spark** | Distributed data transformation, submitted via `spark-submit` on K8s |
| **Apache Superset** | BI dashboards, SQL Lab, PVC-backed metadata store |
| **Docker** | Custom images per service with pinned dependencies baked in |
| **Helm** | Package management for Airflow |

## What this project demonstrates

- **End-to-end data pipeline**: Airflow triggers a Spark job → Spark reads from ClickHouse, transforms the data, writes results back → Superset visualizes the result.
- **Custom Docker images**: each service (Superset, Airflow, Spark) needed specific dependency versions and files baked directly into custom images — not just pip-installed at runtime, but built into the image itself where ephemeral pods require it.
- **Kubernetes RBAC**: Role/RoleBinding resources so Spark's driver pod can dynamically create and manage executor pods.
- **Persistent storage**: ClickHouse, Superset's metadata DB, and Airflow's metadata DB are all backed by PersistentVolumeClaims, so data survives pod restarts (though not a full cluster teardown).

---

## Debugging process

Real infrastructure work is mostly debugging. These are the non-obvious issues that took systematic elimination to actually root-cause — not just "ran a tutorial command."

**1. Silent ClickHouse JDBC failures in Spark**
Spark's ClickHouse JDBC driver failed with an uninformative `SQLException: Query failed` and no chained cause, across two driver versions (0.6.3 and 0.4.6). Isolated the issue by testing the JDBC connection directly (bypassing Spark), comparing `PreparedStatement` vs. plain `Statement` execution, and cross-checking ClickHouse's own `system.query_log`. Root cause: a wire-protocol mismatch between the JDBC driver's execution path and ClickHouse's HTTP interface. Resolved by abandoning JDBC entirely in favor of the `clickhouse-connect` Python client + pandas as a bridge into Spark — a client library already proven reliable in Superset and Airflow.

**2. Kubernetes RBAC for Spark's driver pod**
Spark's driver needs to create/delete its own executor pods, services, and configmaps via the Kubernetes API — the default ServiceAccount has none of these permissions. Diagnosed via `Forbidden` errors in the driver logs, then wrote a minimal Role/RoleBinding granting exactly the required verbs (`create`, `delete`, `deletecollection`, etc.) on the specific resources needed.

**3. Cross-service Python dependency conflicts**
`ModuleNotFoundError`/`TypeError` chain caused by Python 3.8 (Spark's base image) being incompatible with a modern `clickhouse-connect` release that used newer type-hint syntax (`list[X]` requires Python 3.9+). Fixed by pinning to a compatible package version rather than upgrading the base image's Python (which itself turned into a rabbit hole — Debian-based images don't support Ubuntu's `deadsnakes` PPA, and building Python from source added more risk than it was worth).

**4. Silent file-permission bug**
A downloaded JDBC driver `.jar` was owned by `root` with `600` permissions, silently blocking the non-root `spark` user from loading it — surfaced as a misleading `ClassNotFoundException` that looked like a missing dependency rather than a permissions issue.

**5. DAGs must be baked into the image, not `kubectl cp`'d in**
The most subtle bug of the whole project: DAG files were being copied live into the scheduler and dag-processor pods via `kubectl cp`, which worked for *those* pods — but KubernetesExecutor launches a brand-new, separate pod for every actual task run, and that worker pod does **not** share a filesystem with the scheduler. Worker pods start from a fresh copy of the image and look for DAGs in their own `/opt/airflow/dags/` — if the file was only copied to the scheduler, the worker can't find it, logs `"Dag not found during start up"`, and silently marks the task `up_for_reschedule` forever, with no visible error in the UI (since the pod is deleted before its logs can be fetched). Root-caused by setting `delete_worker_pods: 'False'` to keep a failed pod alive long enough to inspect its logs directly. Fixed permanently by baking DAG files into the Airflow image itself via `COPY airflow/dags/ /opt/airflow/dags/`, so every pod type gets an identical, consistent copy.

**6. Ephemeral storage masquerading as "it's just flaky"**
Superset's Postgres initially had no PersistentVolumeClaim, so any pod recreation — a crash, a manual fix-attempt delete, or certain restart sequences — wiped all saved dashboards/charts/users back to zero, while looking like an unrelated bug (a "themes does not exist" migration error) each time. Fixed by adding a proper PVC-backed Postgres deployment, matching the pattern already used for ClickHouse.

**7. Zombie scheduler state after database resets**
An overly broad `DROP SCHEMA public CASCADE` reset on Airflow's Postgres left a core internal table (`job`, used for scheduler heartbeat tracking) missing, causing the scheduler pod to fail its Kubernetes startup probe and crash-loop — which in turn caused any task running at the time to get orphaned and permanently stuck in `up_for_reschedule`. Diagnosed via `kubectl describe pod` event history and Postgres table listings; fixed with `airflow db reset -y` for a complete, correct schema rebuild.

---

## Repository structure

```
upbase-data-platform/
├── k8s/                         # Kubernetes manifests (Deployments, Services, PVCs, RBAC)
│   ├── clickhouse.yaml
│   ├── superset.yaml
│   ├── superset-postgres.yaml
│   └── spark-rbac.yaml
├── docker/                      # Custom Dockerfiles per service
│   ├── superset.Dockerfile
│   ├── airflow.Dockerfile
│   └── spark.Dockerfile
├── airflow/
│   ├── values.yaml               # Helm values for Airflow (KubernetesExecutor config)
│   └── dags/
│       ├── clickhouse_test_dag.py
│       ├── generate_data_append_dag.py
│       ├── generate_data_reset_dag.py
│       └── spark_aggregate_dag.py
├── spark-jobs/
│   └── aggregate_events.py
├── scripts/
│   ├── add_dags.sh               # Fast local iteration — does NOT replace image rebuild
│   └── clean_dags.sh             # Clean completed pods  
└── README.md
```

---

## Setup — full clean build

### Prerequisites
Docker, `kubectl`, `kind`, `helm`, all installed on the host/VM.

### 1. Create the cluster
```bash
kind create cluster --name temp_ns
kubectl get nodes       # confirm Ready
```

### 2. Deploy ClickHouse
```bash
kubectl apply -f k8s/clickhouse.yaml
kubectl get pods -w     # wait for 1/1 Running
```

Create the schema and seed sample data:
```bash
kubectl exec -it $(kubectl get pods -o name | grep clickhouse | cut -d/ -f2) -- clickhouse-client --password upbase123
```
```sql
CREATE DATABASE upbase;

CREATE TABLE upbase.events (
    event_time DateTime,
    user_id UInt32,
    event_type String
) ENGINE = MergeTree()
ORDER BY event_time;

CREATE TABLE upbase.event_counts (
    event_type String,
    count UInt64
) ENGINE = MergeTree()
ORDER BY event_type;

INSERT INTO events (event_time, user_id, event_type)
SELECT
    now() - INTERVAL rand() % 30 DAY,
    (rand() % 500) + 1,
    ['login', 'logout', 'purchase', 'signup', 'click', 'view_page'][(rand() % 6) + 1]
FROM numbers(2000);
```

### 3. Build and load the Superset image, create its ConfigMap
```bash
docker build -t upbase/superset:local -f docker/superset.Dockerfile .
kind load docker-image upbase/superset:local --name temp_ns
kubectl create configmap superset-config --from-file=superset_config.py
```

### 4. Deploy Superset + its (PVC-backed) Postgres
```bash
kubectl apply -f k8s/superset-postgres.yaml
kubectl apply -f k8s/superset.yaml
kubectl get pods -w
```
```bash
kubectl exec -it deploy/superset -- superset db upgrade
kubectl exec -it deploy/superset -- superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin
kubectl exec -it deploy/superset -- superset init
```
(Note: this Superset CLI version does not accept `--role` on `fab create-admin` — omit it, Admin is the default.)

### 5. Build and load the Airflow image (DAGs baked in), deploy via Helm
```bash
docker build --no-cache -t upbase/airflow:local -f docker/airflow.Dockerfile .
kind load docker-image upbase/airflow:local --name temp_ns

helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm install airflow apache-airflow/airflow -n default -f airflow/values.yaml --timeout 15m
kubectl get pods -w
```
```bash
kubectl exec -it $(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2) -- \
  airflow users create --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin --role Admin

for dag in clickhouse_test_dag generate_data_append_dag generate_data_reset_dag spark_aggregate_dag; do
  kubectl exec -it $(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2) -- \
    airflow dags unpause "$dag"
done
```

> **Critical**: DAG files must be baked into the image (`COPY airflow/dags/ /opt/airflow/dags/` in `docker/airflow.Dockerfile`)

### 6. Build and load the Spark image, apply RBAC
```bash
docker build -t upbase/spark:local -f docker/spark.Dockerfile .
kind load docker-image upbase/spark:local --name temp_ns
kubectl apply -f k8s/spark-rbac.yaml
```

### 7. Verify a clean baseline before testing anything
```bash
kubectl get pods
```
Every pod `Running`, `RESTARTS = 0`.

### 8. Access the UIs
```bash
kubectl port-forward svc/superset-svc 8088:8088
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace default
```
- Superset: http://localhost:8088 (admin/admin)
- Airflow: http://localhost:8080 (admin/admin)

Re-add the ClickHouse connection in Superset (host `clickhouse`, port `8123`, db `upbase`, user `default`, password `upbase123`, SSL off).

### 9. Smoke-test each DAG once, manually
```bash
kubectl exec -it $(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2) -- airflow dags trigger clickhouse_test_dag
kubectl exec -it $(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2) -- airflow dags trigger generate_data_append_dag
kubectl exec -it $(kubectl get pods -o name | grep airflow-scheduler | cut -d/ -f2) -- airflow dags trigger spark_aggregate_dag
```
Confirm each completes and ClickHouse row counts change accordingly, with zero new pod restarts anywhere in the cluster.

---

## Quick full-reset one-liner

```bash
kind delete cluster --name temp_ns
kind create cluster --name temp_ns
# then continue from step 2 above
```

