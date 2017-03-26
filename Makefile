/home/fiatjaf/comp/go/pkg/linux_amd64/github.com/fiatjaf/go-rocksdown.a: rocksdown.go
	CGO_CFLAGS="-I/home/fiatjaf/comp/rocksdb/include" CGO_LDFLAGS="-L/home/fiatjaf/comp/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" go get

test:
	CGO_CFLAGS="-I/home/fiatjaf/comp/rocksdb/include" CGO_LDFLAGS="-L/home/fiatjaf/comp/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" go test
