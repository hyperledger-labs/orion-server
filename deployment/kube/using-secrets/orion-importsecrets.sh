#!/bin/bash
# CA certificate
kubectl create secret generic cacert --from-file=deployment/crypto/CA/CA.pem -o yaml --namespace=orion-ns
# Admin user certificate
kubectl create secret generic admincert --from-file=deployment/crypto/admin/admin.pem -o yaml --namespace=orion-ns
# Server certificate and private key
kubectl create secret generic servercert --from-file=deployment/crypto/server/ -o yaml  --namespace=orion-ns
# Orion Local and shared configuration
kubectl create secret generic orion-config-files --from-file=deployment/config-docker/ -o yaml  --namespace=orion-ns

