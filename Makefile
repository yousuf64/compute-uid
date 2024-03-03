create-cluster:
	kind create cluster --config deploy/kind-cluster.yaml
cluster-load-images:
	kind load docker-image --name compute-uid-cluster \
 		yousuf64/compute-uid-server:1.8 \
 		mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest
cluster-deploy-contour:
	kubectl apply -f https://projectcontour.io/quickstart/contour.yaml;
	kubectl patch daemonsets -n projectcontour envoy -p \
		'{"spec":{"template":{"spec":{"nodeSelector":{"ingress-ready":"true"},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Equal","effect":"NoSchedule"},{"key":"node-role.kubernetes.io/master","operator":"Equal","effect":"NoSchedule"}]}}}}'
delete-cluster:
	kind delete cluster --name compute-uid-cluster
envoy:
	docker run --rm -it -v $(CURDIR)/envoy.yaml:/envoy.yaml -p 80:80 envoyproxy/envoy:dev -c envoy.yaml
cosmos:
	docker run -p 8081:8081 --memory 3g --cpus=4.0 --name=cosmos-db --env AZURE_COSMOS_EMULATOR_PARTITION_COUNT=3 --env AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=true \
	--interactive --tty mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator