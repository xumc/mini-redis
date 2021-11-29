export GO111MODULE=on
export GOPROXY=https://goproxy.io,direct

build:
	go build -v -o mini-redis

build-linux:
	GOOS=linux GOARCH=amd64 go build -v -o mini-redis

build-linux-perf:
	cd performance && GOOS=linux GOARCH=amd64 go build -v -o  perf
