package common

import "os"

const TableIdsKey = "tableIds"

//目录是否存在--rocksdb使用
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
