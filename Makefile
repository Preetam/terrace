SOURCES = $(shell find . -type f -name '*.go' -not -path './vendor/*')

build/terrace: $(SOURCES)
	GOOS=linux GOARCH=amd64 go build -o $@ ./cmd/terrace

build/docker-image: build/terrace Dockerfile
	docker build . -t preetamjinka/terrace:latest
	docker save --output $@ preetamjinka/terrace:latest

.PHONY: docker-image
docker-image: build/docker-image
