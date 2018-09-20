build/terrace-gen:
	GOOS=linux GOARCH=amd64 go build -o $@

.PHONY: docker-image
docker-image: build/terrace-gen
	docker build . -t preetamjinka/terrace-gen:latest
