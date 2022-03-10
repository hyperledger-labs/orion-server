# Kubernetes deployments
This folder contains two example single pod Orion kubernetes deployments
In the near future, StatefulSet based deployments will be added, using raft replication. 

# Minimal test deployment
`kubectl apply -f single-file/orion-minimal.yaml`
Creates namespace `orion-ns`, three persistent volumes claims: `orion-crypto-data`, `orion-ledger-data` and `orion-config-data`.
Creates three persistent volumes, one for each volume claim, used for crypto, ledger and config. 
After that deployment `orion` created, with one single Orion pod from `orionbcdb/orion-server` image. 
This deployment exposed using service with type `NodePort`. 

>Data on crypto volume created using `initContainers`, so certificates (crypto) volume will be regenerated each time orion pod restarts, i.e. no real data persistence. 

##Multi-file kubernetes deployment
Run from `orion-server` root folder:
1. `kubectl apply -f deployment/kube/using-secrets/orion-namespace.yaml` - create orion namespace `orion-ns`
2. `deployment/kube/using-secrets//orion-importsecrets.sh` - certificates and configs stored inside secrets, for future mapping to volumes 
> In case of real world deployment, don't forget to update files/paths to actual certificates and configuration files
3. `kubectl apply -f deployment/kube/using-secrets/orion-volumes.yaml` - persistent volume claim for ledger data
4. `kubectl apply -f deployment/kube/using-secrets/orion-deployment.yaml` - create deployment with mapping configuration and certificates to volumes
5. `kubectl apply -f deployment/kube/using-secrets/orion-service.yaml` - expose deployment using `NodePort`

