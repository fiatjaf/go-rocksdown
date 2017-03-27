package rocksdown

import (
	"github.com/fiatjaf/levelup"
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
	if data.Size() == 0 {
		return "", levelup.NotFound
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

func (r RocksDown) ReadRange(opts *levelup.RangeOpts) levelup.ReadIterator {
	if opts == nil {
		opts = &levelup.RangeOpts{}
	}
	opts.FillDefaults()

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := r.db.NewIterator(ro)

	it.Seek([]byte(opts.Start))

	if opts.Reverse {
		if opts.End == levelup.DefaultRangeEnd {
			it.SeekToLast()
		} else {
			it.Seek([]byte(opts.End))
			it.Prev()
		}
	}

	if opts.Limit <= 0 {
		opts.Limit = 9999999
	}

	return &ReadIterator{
		iter:  it,
		opts:  opts,
		count: 1,
	}
}

type ReadIterator struct {
	iter  *gorocksdb.Iterator
	opts  *levelup.RangeOpts
	count int
}

func (ri *ReadIterator) Valid() bool {
	if !ri.iter.Valid() {
		return false
	}
	if ri.count > ri.opts.Limit {
		return false
	}
	if ri.opts.Reverse {
		if string(ri.iter.Key().Data()) < ri.opts.Start /* inclusive */ {
			return false
		}
	} else {
		if string(ri.iter.Key().Data()) >= ri.opts.End /* not inclusive */ {
			return false
		}
	}
	return true
}

func (ri *ReadIterator) Next() {
	ri.count++
	if ri.opts.Reverse {
		ri.iter.Prev()
	} else {
		ri.iter.Next()
	}
}

func (ri *ReadIterator) Key() string   { return string(ri.iter.Key().Data()) }
func (ri *ReadIterator) Value() string { return string(ri.iter.Value().Data()) }
func (ri *ReadIterator) Error() error  { return ri.iter.Err() }
func (ri *ReadIterator) Release()      { ri.iter.Close() }
