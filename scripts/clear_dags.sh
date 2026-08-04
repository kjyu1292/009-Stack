kubectl delete pod $(kubectl get pods | grep generate-data-append-dag | awk '{print $1}')
kubectl delete pod $(kubectl get pods | grep generate-data-reset-dag | awk '{print $1}')
kubectl delete pod $(kubectl get pods | grep main-generation | awk '{print $1}')
