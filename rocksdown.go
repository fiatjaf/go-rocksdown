package rocksdown

// #cgo CFLAGS: -I/home/fiatjaf/comp/rocksdb/include
// #cgo LDFLAGS: -L/home/fiatjaf/comp/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
import "C"
import (
	"github.com/fiatjaf/go-levelup"
	"github.com/tecbot/gorocksdb"
)

type RocksDown struct {
	db *gorocksdb.DB
}

func NewDatabase(path string) levelup.DB {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		panic(err)
	}
	return &RocksDown{db}
}

func (r RocksDown) Put(key, value string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Put(wo, []byte(key), []byte(value))
}

func (r RocksDown) Get(key string) (string, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	data, err := r.db.Get(ro, []byte(key))
	if err != nil {
		return "", err
	}
	return string(data.Data()), nil
}

func (r RocksDown) Del(key string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Delete(wo, []byte(key))
}

func (r RocksDown) Batch(ops []levelup.Operation) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wb := gorocksdb.NewWriteBatch()
	for _, op := range ops {
		switch op["type"] {
		case "put":
			wb.Put([]byte(op["key"]), []byte(op["value"]))
		case "del":
			wb.Delete([]byte(op["key"]))
		}
	}
	return r.db.Write(wo, wb)
}

func (r RocksDown) ReadRange(opts levelup.RangeOpts) levelup.ReadIterator {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := r.db.NewIterator(ro)

	if opts.Start != "" {
		it.Seek([]byte(opts.Start))
	}
	if opts.Limit <= 0 {
		opts.Limit = 9999999
	}

	return &ReadIterator{
		iter:    it,
		opts:    opts,
		scanned: 0,
	}
}

type ReadIterator struct {
	iter    *gorocksdb.Iterator
	opts    levelup.RangeOpts
	scanned int
}

func (ri *ReadIterator) Next() bool {
	if !ri.iter.Valid() {
		return false
	}

	ri.iter.Next()
	ri.scanned++
	if string(ri.iter.Key().Data()) >= ri.opts.End {
		return false
	}

	return true
}

func (ri *ReadIterator) Key() string   { return string(ri.iter.Key().Data()) }
func (ri *ReadIterator) Value() string { return string(ri.iter.Value().Data()) }
func (ri *ReadIterator) Error() error  { return ri.iter.Err() }
func (ri *ReadIterator) Release()      { ri.iter.Close() }
