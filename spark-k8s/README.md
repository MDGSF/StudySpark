# spark k8s

- <https://github.com/kubeflow/spark-operator>
- <https://www.kubeflow.org/docs/components/spark-operator/getting-started/>

```sh
kubectl apply -f spark-pi.yaml
kubectl delete -f spark-pi.yaml
kubectl get sparkapplication spark-pi -o=yaml
kubectl -n data-platform get sparkapplication spark-pi -o=yaml
kubectl -n data-platform describe sparkapplication spark-pi
```

