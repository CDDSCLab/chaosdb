package kv

//键值结构 主要使用在迭代时返回
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}

//对迭代器的接口封装，对上一层提供统一接口
type RowsIterator interface {
	//获取迭代器当前指针指向的键值对的键
	Key() []byte
	//获取迭代器当前指针指向的键值对的值
	Value() []byte
	//移动迭代器指针 下一个
	Next()
	//当前迭代器是否有效
	Valid() bool
	//当前迭代器指向的键是否符合前缀要求
	ValidForPrefix(prefix []byte) bool
	//关闭迭代器
	Close()
	//移动迭代器指针到指定的key
	Seek(key []byte) RowsIterator
}

type Storage interface {
	//获取对应键的值
	Get(key []byte) ([]byte, error)
	//批量获取键的值
	BatchGet(keys [][]byte) ([][]byte, error)
	//获取指定范围的键值
	Scan(startKey,endKey []byte, limit int) []Pair
	//创建迭代器
	NewScanIterator(startKey, endKey []byte) RowsIterator
	//存储键值
	Put(key, value []byte) error
	//批量存储
	BatchPut(keys, values [][]byte) error
	//删除键
	Delete(key []byte) error
	//批量删除
	BatchDelete(keys [][]byte) error
	//关闭数据库文件
	Close()error
}
