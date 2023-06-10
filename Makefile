envoy:
	docker run --rm -it -v $(CURDIR)/envoy.yaml:/envoy.yaml -p 80:80 envoyproxy/envoy:dev -c envoy.yaml