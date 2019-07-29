#!/bin/sh

cd /go/src
go get "github.com/akkeris/vault-client"
go get "github.com/lib/pq"
cd /go/src/rabbitmq-scanner
go build rabbitmq-scanner.go

