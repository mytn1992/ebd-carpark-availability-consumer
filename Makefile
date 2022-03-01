.PHONY: build clean deploy gomodgen

run:
	export GO111MODULE=on && go run main.go 

test:
	export GO111MODULE=on && go test -v ./...

build: 
	export GO111MODULE=on
	go build -ldflags="-s -w" -o bin/main main.go

deploy:
	sls deploy --verbose --force
