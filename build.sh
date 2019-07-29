#!/bin/sh -e
curl -O 'https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz'
tar xf 'go1.12.7.linux-amd64.tar.gz'

mkdir -p gopath

export GOROOT="`readlink -f ./go/`"
export GOPATH="`readlink -f ./gopath/`"
export PATH="`readlink -f ./go/bin/`:$PATH"

go get "golang.org/x/net/html/charset"
go get --insecure "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

# required by sqlite
export CGO_ENABLED=1

GOARCH=386 go build -o parser_32.bin parser.go
GOARCH=amd64 go build -o parser_64.bin parser.go
