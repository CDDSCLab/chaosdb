package leveldb

import (
	"fmt"
	"testing"

	"github.com/CDDSCLab/chaosdb/common/kv"
	"github.com/CDDSCLab/chaosdb/store/common"

	jsoniter "github.com/json-iterator/go"
	"github.com/pingcap/tidb/model"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type LevelDBSuite struct {
	storage kv.Storage
}

var jsonIter = jsoniter.ConfigCompatibleWithStandardLibrary

var _ = Suite(&LevelDBSuite{})

func (s *LevelDBSuite) SetUpTest(c *C) {
	var err error
	s.storage, err = NewLevelDB("./", "leveldb_data")
	c.Assert(err, IsNil)
}

func (s *LevelDBSuite) TestKit(c *C) {
	s.mustPutOK(c)
	s.mustBatchPutOk(c)
	s.mustGetOK(c)
	s.mustBatchGetOK(c)
	s.mustDeleteOK(c)
	s.mustBatchDeleteOK(c)
	s.mustScanOK(c)
	s.mustFuzzyGetOK(c)
}

func (s *LevelDBSuite) mustPutOK(c *C) {
	key := []byte(common.TableIdsKey)
	ids := []int64{1, 2, 3, 4, 5}
	idsJson, err := jsonIter.Marshal(ids)
	c.Assert(err, IsNil)
	err = s.storage.Put(key, idsJson)
	c.Assert(err, IsNil)
}
func (s *LevelDBSuite) mustBatchPutOk(c *C) {
	key1 := "1_1"
	key2 := "1_2"
	key3 := "1_3"
	value1, _ := jsoniter.Marshal(&model.TableInfo{ID: 1, Name: model.NewCIStr("user_1")})
	value2, _ := jsoniter.Marshal(&model.TableInfo{ID: 1, Name: model.NewCIStr("user_2")})
	value3, _ := jsoniter.Marshal(&model.TableInfo{ID: 1, Name: model.NewCIStr("user_3")})
	var keys, values [][]byte
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3))
	values = append(values, value1, value2, value3)
	err := s.storage.BatchPut(keys, values)
	c.Assert(err, IsNil)
}

func (s *LevelDBSuite) mustGetOK(c *C) {
	key := []byte(common.TableIdsKey)
	value, err := s.storage.Get(key)
	c.Assert(err, IsNil)
	var ids []int64
	err = jsoniter.Unmarshal(value, &ids)
	c.Assert(err, IsNil)
	fmt.Println(ids)
}

func (s *LevelDBSuite) mustBatchGetOK(c *C) {
	key1 := "1_1"
	key2 := "1_2"
	key3 := "1_3"
	var keys, values [][]byte
	var val model.TableInfo
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3))
	values, err := s.storage.BatchGet(keys)
	c.Assert(err, IsNil)
	for _, value := range values {
		jsoniter.Unmarshal(value, &val)
		fmt.Println(val)
	}
}

func (s *LevelDBSuite) mustDeleteOK(c *C) {
	key := []byte(common.TableIdsKey)
	err := s.storage.Delete(key)
	c.Assert(err, IsNil)
	s.storage.Get(key)
	c.Assert(err, NotNil)
}

func (s *LevelDBSuite) mustBatchDeleteOK(c *C) {
	key1 := "1_1"
	key2 := "1_2"
	key3 := "1_3"
	var keys [][]byte
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3))
	err := s.storage.BatchDelete(keys)
	c.Assert(err, IsNil)
	s.storage.Get([]byte(key1))
	c.Assert(err, NotNil)
}

func (s *LevelDBSuite) mustScanOK(c *C) {
	key1 := "1_1"
	key2 := "1_2"
	key3 := "1_3"
	key4 := "1_4"
	key5 := "1_5"
	key6 := "1_6"
	var keys, values [][]byte
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3), []byte(key4), []byte(key5), []byte(key6))
	values = append(values, []byte("user_1"), []byte("user_2"), []byte("user_3"), []byte("user_4"), []byte("user_5"), []byte("user_6"))
	var err error
	err = s.storage.BatchPut(keys, values)
	c.Assert(err, IsNil)
	//左闭右开 如果想全部迭代，传入空字节数组
	pairs := s.storage.Scan([]byte(key1), []byte{}, 6)
	for i, pair := range pairs {
		c.Assert(pair.Err, IsNil)
		fmt.Println("i-key:", i, string(pair.Key))
		fmt.Println("i-value:", i, string(pair.Value))
	}
	err = s.storage.BatchDelete(keys)
	c.Assert(err, IsNil)
}

func (s *LevelDBSuite) mustFuzzyGetOK(c *C) {
	fmt.Println("=====mustFuzzyGetOK=======")
	key1 := "1_1_18_1"
	key2 := "1_2_zhangsan_1"
	key3 := "1_2_lizi_2"
	key4 := "1_2_zhangsan_3"
	key5 := "1_1_20_2"
	key6 := "1_1_30_3"
	var keys, values [][]byte
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3), []byte(key4), []byte(key5), []byte(key6))
	values = append(values, []byte("1_1"), []byte("1_2"), []byte("1_2"), []byte("1_2"), []byte("1_1"), []byte("1_1"))
	var err error
	err = s.storage.BatchPut(keys, values)
	c.Assert(err, IsNil)
	//模糊查询
	pairs := s.storage.Scan([]byte("1_2_zhangsan"), []byte{}, 100)
	for i, pair := range pairs {
		c.Assert(pair.Err, IsNil)
		fmt.Println("i-key:", i, string(pair.Key))
		fmt.Println("i-value:", i, string(pair.Value))
	}
	s.storage.BatchDelete(keys)
}

func (s *LevelDBSuite) TestScan(c *C) {
	s.scanIter(c)
}

func (s *LevelDBSuite) scanIter(c *C) {
	fmt.Println("------ScanIter---------")

	key1 := "tb_r_1_1"
	key2 := "tb_r_1_2"
	key3 := "tb_r_1_3"
	key4 := "tb_r_1_4"
	key5 := "tb_r_1_5"
	key6 := "tb_r_1_6"
	key7 := "tb_r_1_7"
	key8 := "tb_r_1_8"
	key9 := "tb_r_1_9"
	key10 := "tb_r_1_10"
	key11 := "tb_r_1_11"
	var keys, values [][]byte
	keys = append(keys, []byte(key1), []byte(key2), []byte(key3), []byte(key4), []byte(key5), []byte(key6), []byte(key7), []byte(key8), []byte(key9), []byte(key10), []byte(key11))
	values = append(values, []byte("aaa"), []byte("aaa"), []byte("aaa"), []byte("aaa"), []byte("aaa"), []byte("1_1"), []byte("1_2"), []byte("1_2"), []byte("1_2"), []byte("1_1"), []byte("1_1"))
	var err error
	err = s.storage.BatchPut(keys, values)
	c.Assert(err, IsNil)

	kvs := s.storage.Scan([]byte("tb_r_1_3"), []byte("tb_r_1_5"), 1000)
	for _, kv := range kvs {
		kk := string(kv.Key)
		vv := string(kv.Value)
		fmt.Println("key:", kk, ",value:", vv)
	}

	rowIter := s.storage.NewScanIterator([]byte("tb_r_1_3"), []byte("tb_r_1_5"))
	fmt.Println(rowIter.Valid())

	for ; rowIter.Valid(); rowIter.Next() {
		fmt.Printf("Key: %v Value: %v\n", string(rowIter.Key()), string(rowIter.Value()))
	}
	s.storage.BatchDelete(keys)
}
