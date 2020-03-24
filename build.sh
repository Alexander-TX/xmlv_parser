#!/bin/bash -ex
wget -N 'https://dl.google.com/go/go1.14.linux-amd64.tar.gz'

rm -rf go

tar xf 'go1.14.linux-amd64.tar.gz'

mkdir -p bin src

export GOROOT="`readlink -f ./go/`"
export GOPATH="`readlink -f .`"
export PATH="`readlink -f ./go/bin/`:$PATH"
export GOBIN="$GOPATH/bin"

export GIT_SSL_NO_VERIFY=1
go get "golang.org/x/text/encoding"
go get "golang.org/x/text/encoding/charmap"
go get "golang.org/x/net/html/charset"
go get --insecure "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

# required by sqlite
export CGO_ENABLED=1

mkdir -p bin src

VER=1.0-$(git rev-parse --short HEAD)

LDFLAGS=-ldflags="-X main.EltexPackageVersion=$VER"

GOARCH=386 go build "$LDFLAGS" -o parser_32.bin parser.go
GOARCH=amd64 go build "$LDFLAGS" -o parser_64.bin parser.go

GOARCH=386 go build "$LDFLAGS" -o jtvgen_32.bin jtvgen.go
GOARCH=amd64 go build "$LDFLAGS" -o jtvgen_64.bin jtvgen.go

# Online guide suggests to do this, but it does not work because
# of https://github.com/golang/go/issues/11778 and other reasons
#GOOS=windows GOARCH=386 go install

GOOS=windows CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc \
 GOARCH=386 go build "$LDFLAGS" -o parser.exe parser.go
GOOS=windows CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc \
 GOARCH=386 go build "$LDFLAGS" -o jtvgen.exe jtvgen.go
