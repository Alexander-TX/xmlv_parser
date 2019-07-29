#!/bin/sh -e
go get "golang.org/x/net/html/charset"
go get --insecure "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

# required by sqlite
export CGO_ENABLED=1

GOARCH=386 go build -o parser_32 parser.go
GOARCH=amd64 go build -o parser_64 parser.go
