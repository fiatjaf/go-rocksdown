package rocksdown

import (
	"bytes"
	"os"

	"github.com/fiatjaf/levelup"
	"github.com/tecbot/gorocksdb"
)

type RocksDown struct {
	db   *gorocksdb.DB
	path string
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
	return &RocksDown{db, path}
}

func (r RocksDown) Close() { r.db.Close() }
func (r RocksDown) Erase() {
	r.Close()
	os.RemoveAll(r.path)
}

func (r RocksDown) Put(key, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Put(wo, key, value)
}

func (r RocksDown) Get(key []byte) ([]byte, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	data, err := r.db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	if data.Size() == 0 {
		return nil, levelup.NotFound
	}
	return data.Data(), nil
}

func (r RocksDown) Del(key []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Delete(wo, key)
}

func (r RocksDown) Batch(ops []levelup.Operation) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wb := gorocksdb.NewWriteBatch()
	for _, op := range ops {
		switch op.Type {
		case "put":
			wb.Put(op.Key, op.Value)
		case "del":
			wb.Delete(op.Key)
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

	it.Seek(opts.Start)

	if opts.Reverse {
		if bytes.Compare(opts.End, levelup.DefaultRangeEnd) == 0 {
			it.SeekToLast()
		} else {
			it.Seek(opts.End)
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
		if bytes.Compare(ri.iter.Key().Data(), ri.opts.Start) == -1 /* inclusive */ {
			return false
		}
	} else {
		if bytes.Compare(ri.iter.Key().Data(), ri.opts.End) >= 0 /* not inclusive */ {
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

func (ri *ReadIterator) Key() []byte   { return ri.iter.Key().Data() }
func (ri *ReadIterator) Value() []byte { return ri.iter.Value().Data() }
func (ri *ReadIterator) Error() error  { return ri.iter.Err() }
func (ri *ReadIterator) Release()      { ri.iter.Close() }
