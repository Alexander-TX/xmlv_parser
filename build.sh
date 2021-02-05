#!/bin/bash -ex

export LANG=C

ELT_GO_VERSION=1.14.4

wget -N "https://dl.google.com/go/go${ELT_GO_VERSION}.linux-amd64.tar.gz"

rm -rf go

tar xf "go${ELT_GO_VERSION}.linux-amd64.tar.gz"

mkdir -p bin src

export GOROOT="`readlink -f ./go/`"
export GOPATH="`readlink -f .`"
export PATH="`readlink -f ./go/bin/`:$PATH"
export GOBIN="$GOPATH/bin"

export GIT_SSL_NO_VERIFY=1
go get "golang.org/x/text/encoding"
go get "golang.org/x/text/encoding/charmap"
go get "golang.org/x/net/html/charset"
go get --insecure "github.com/Alexander-TX/go-sqlite3"

# required by sqlite
export CGO_ENABLED=1

mkdir -p bin src

ELT_VER=1.1.6
VER=$ELT_VER-$(git rev-parse --short HEAD)
BUILDER="$(whoami)@$(hostname)"
BUILD_TIME="$(date +'%d %B %Y %H:%M')"

LDFLAGS=-ldflags="-X main.EltexPackageVersion=$VER -X \"main.EltexBuilder=$BUILDER\"  -X \"main.EltexBuildTime=$BUILD_TIME\""

GOARCH=386 go build "$LDFLAGS" -o parser-$ELT_VER-x32.bin parser.go platform_default.go
GOARCH=amd64 go build "$LDFLAGS" -o parser-$ELT_VER-x64.bin parser.go platform_default.go

GOARCH=386 go build "$LDFLAGS" -o jtvgen-$ELT_VER-x32.bin jtvgen.go platform_default.go
GOARCH=amd64 go build "$LDFLAGS" -o jtvgen-$ELT_VER-x64.bin jtvgen.go platform_default.go

GOARCH=386 go build "$LDFLAGS" -o epgx-$ELT_VER-x32.bin epgx.go platform_default.go
GOARCH=amd64 go build "$LDFLAGS" -o epgx-$ELT_VER-x64.bin epgx.go platform_default.go

# Online guide suggests to do this, but it does not work because
# of https://github.com/golang/go/issues/11778 and other reasons
#GOOS=windows GOARCH=386 go install

GOOS=windows CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc \
 GOARCH=386 go build "$LDFLAGS" -o parser-$ELT_VER.exe parser.go platform_windows.go
GOOS=windows CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc \
 GOARCH=386 go build "$LDFLAGS" -o jtvgen-$ELT_VER.exe jtvgen.go platform_windows.go
GOOS=windows CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc \
 GOARCH=386 go build "$LDFLAGS" -o epgx-$ELT_VER.exe epgx.go platform_windows.go
