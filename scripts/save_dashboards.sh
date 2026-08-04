kubectl exec -it deploy/superset -- superset export-dashboards -f /tmp/dashboard_export.zip
kubectl cp default/$(kubectl get pods -o name | grep superset- | grep -v db | cut -d/ -f2):/tmp/dashboard_export.zip ~/projects/009-stack/backups/dashboard_export.zip
