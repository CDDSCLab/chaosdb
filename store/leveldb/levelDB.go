package leveldb

import (
	"bytes"
	"errors"
	"strings"
	"sync"

	"github.com/CDDSCLab/chaosdb/common/kv"
	"github.com/CDDSCLab/chaosdb/comparator"
	"github.com/CDDSCLab/chaosdb/store/common"
	"github.com/CDDSCLab/chaosdb/table"
	"github.com/CDDSCLab/chaosdb/util/stringutil"

	jsoniter "github.com/json-iterator/go"
	"github.com/op/go-logging"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var leveldbLogger = logging.MustGetLogger("levelDB")

type LevelIter struct {
	iterator iterator.Iterator
	valid    bool
}

func (iter *LevelIter) Close() {
	iter.iterator.Release()
}

func (iter *LevelIter) Key() []byte {
	src := iter.iterator.Key()
	return stringutil.MakeCopy(src)
}

func (iter *LevelIter) Value() []byte {
	src := iter.iterator.Value()
	return stringutil.MakeCopy(src)
}

func (iter *LevelIter) Next() {
	iter.valid = iter.iterator.Next()
}

func (iter *LevelIter) Valid() bool {
	return iter.valid
}

func (iter *LevelIter) ValidForPrefix(prefix []byte) bool {
	return bytes.HasPrefix(iter.iterator.Key(), prefix)
}

func (iter *LevelIter) Seek(key []byte) kv.RowsIterator {
	iter.iterator.Seek(key)
	return iter
}

type LevelDB struct {
	db         *leveldb.DB
	TableIds   []uint64
	TableInfos map[string]*table.MyTableInfo
	mu         sync.RWMutex
}

func (ld *LevelDB) Get(key []byte) ([]byte, error) {
	ld.mu.RLock()
	defer ld.mu.RUnlock()
	value, err := ld.db.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return nil, nil
	}
	return stringutil.MakeCopy(value), err
}

func (ld *LevelDB) BatchGet(keys [][]byte) ([][]byte, error) {
	var values [][]byte
	for _, key := range keys {

		value, err := ld.db.Get(key, nil)
		if err != nil {
			leveldbLogger.Warningf("[levelDB][BatchGet] get key(%s) error(%s)", key, err)
			return nil, err
		}
		values = append(values, stringutil.MakeCopy(value))
	}
	return values, nil
}

func (ld *LevelDB) Scan(startKey []byte, endKey []byte, limit int) []kv.Pair {
	ld.mu.RLock()
	iter := ld.db.NewIterator(&util.Range{Start: []byte{}}, nil)
	var pairs []kv.Pair
	iter.Seek(startKey)
	ld.mu.RUnlock()

	for iter.Valid() && len(pairs) < limit {
		key := iter.Key()
		value := iter.Value()
		err := iter.Error()
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			break
		}
		destKey := stringutil.MakeCopy(key)
		destVal := stringutil.MakeCopy(value)
		pairs = append(pairs, kv.Pair{Key: append([]byte{}, destKey...), Value: append([]byte{}, destVal...), Err: err})
		iter.Next()
	}
	return pairs
}

func (ld *LevelDB) NewScanIterator(startKey, endKey []byte) kv.RowsIterator {

	ld.mu.RLock()
	defer ld.mu.RUnlock()

	rangeKey := &util.Range{Start: []byte{}, Limit: endKey}

	ro := &opt.ReadOptions{}
	ro.GetDontFillCache()
	it := ld.db.NewIterator(rangeKey, ro)
	it.Seek(startKey)
	iter := &LevelIter{iterator: it, valid: it.Valid()}

	return iter
}

func (ld *LevelDB) Put(key, value []byte) error {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if value == nil {
		err := errors.New("value is can not be nil")
		return err
	}
	err := ld.db.Put(key, value, nil)
	if err != nil {
		leveldbLogger.Errorf("[levelDB][Put] Put key(%s) error(%s)", key, err)
		return err
	}
	return nil
}

func (ld *LevelDB) BatchPut(keys, values [][]byte) error {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	batch := &leveldb.Batch{}
	for i, key := range keys {
		value := values[i]
		if value == nil {
			err := errors.New("value is can not be nil")
			return err
		}
		batch.Put(key, value)
	}
	err := ld.db.Write(batch, nil)
	if err != nil {
		leveldbLogger.Errorf("[levelDB][BatchPut] error(%s)", err)
		return err
	}
	return nil
}

func (ld *LevelDB) Delete(key []byte) error {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	err := ld.db.Delete(key, nil)
	if err != nil {
		leveldbLogger.Errorf("[levelDB][Delete] delete key(%s) error(%s)", key, err)
		return err
	}
	return nil
}

func (ld *LevelDB) BatchDelete(keys [][]byte) error {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	batch := &leveldb.Batch{}
	for _, key := range keys {
		batch.Delete(key)
	}
	err := ld.db.Write(batch, nil)
	if err != nil {
		leveldbLogger.Errorf("[levelDB][BatchDelete] BatchDelete error(%s)", err)
		return err
	}
	return nil
}

func (ld *LevelDB) Close() error {
	return ld.db.Close()
}

func NewLevelDB(path, dbName string) (*LevelDB, error) {
	var (
		d   *leveldb.DB
		err error
	)

	//路径和文件名适配
	if path == "" {
		path = "./leveldb"
	} else {
		path = strings.TrimRight(path, "/")
	}

	if dbName == "" {
		err := errors.New("leveldb dbname is empty")
		return nil, err
	}

	comparator := &comparator.StringAndNumberComparator{}
	opt := &opt.Options{}
	//opt.BlockCacheCapacity = 600 * 1024 * 1024
	opt.Comparer = comparator

	if path == "" {
		d, err = leveldb.Open(storage.NewMemStorage(), opt)
	} else {
		d, err = leveldb.OpenFile(path+"/"+dbName, opt)
	}
	if err != nil {
		leveldbLogger.Errorf("[levelDB][NewLevelDB] Create levelDB Handler error(%s)", err)
		return nil, err
	}

	ld := &LevelDB{db: d}
	//获取表信息
	tableInfoByte, err := ld.Get([]byte(common.TableIdsKey))
	if err != nil {
		leveldbLogger.Errorf("tableInfo get error:%s", err)
		return nil, err
	}
	jsonIter := jsoniter.ConfigCompatibleWithStandardLibrary
	var tableIds []uint64
	if len(tableInfoByte) > 0 {
		err = jsonIter.Unmarshal(tableInfoByte, &tableIds)
		if err != nil {
			leveldbLogger.Errorf("[levelDB][NewLevelDB] unmarshal tableIds err(%s)", err)
			return nil, err
		}
	}
	ld.TableIds = tableIds

	ld.TableInfos = make(map[string]*table.MyTableInfo)

	return ld, nil
}
