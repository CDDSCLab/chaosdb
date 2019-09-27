package levelDB

import (
	"errors"
	"fmt"

	"github.com/CDDSCLab/chaosdb/common/kv"
	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/opt/common"
	common2	"github.com/CDDSCLab/chaosdb/store/common"
	"github.com/CDDSCLab/chaosdb/store/leveldb"
	"github.com/CDDSCLab/chaosdb/table"
	"github.com/CDDSCLab/chaosdb/util/codekey"

	"strconv"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/op/go-logging"
)

type LevelTableOpt struct {
	mu      sync.RWMutex
	leveldb *leveldb.LevelDB
}

var leveldbLogger = logging.MustGetLogger("leveldbOpt")

func NewLevelTableOpt(storage kv.Storage) tableOpt.TableOpt {
	levelTableOpt := &LevelTableOpt{leveldb: storage.(*leveldb.LevelDB)}
	return levelTableOpt
}

func (l *LevelTableOpt) TableExists(tableName string) (bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	tableInfoKey := codekey.EncodeKey(common.Separator, common.TableInfoPrefix, tableName)
	tableInfo, err := l.leveldb.Get(tableInfoKey.Bytes())
	if err != nil {
		return false, err
	}
	if tableInfo == nil {
		errStr := fmt.Sprintf("tableName:%s not found", tableName)
		return false, errors.New(errStr)
	}
	return true, nil
}

func (l *LevelTableOpt) GetUniqTableId() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.leveldb.TableIds) == 0 {
		return 1
	} else {
		lastId := l.leveldb.TableIds[len(l.leveldb.TableIds)-1]
		lastId++
		return uint64(lastId)
	}
}

func (l *LevelTableOpt) GetTableInfo(tableName string) (*table.MyTableInfo, error) {

	//先从缓存获取
	if tableInfo, ok := l.leveldb.TableInfos[tableName]; ok {
		leveldbLogger.Infof("duyong--[GetTableInfo]缓存获取")
		return tableInfo, nil
	}

	//从数据库获取
	tableInfoKey := codekey.EncodeKey(common.Separator, common.TableInfoPrefix, tableName)
	//leveldbLogger.Infof("getTableInfo by name:%s", tableInfoKey.String())
	tableInfoValue, err := l.leveldb.Get(tableInfoKey.Bytes())
	if err != nil || tableInfoValue == nil {
		leveldbLogger.Errorf("leveldb get tableInfo error:%s", err)
		return nil, err
	}
	var tableInfo table.MyTableInfo
	err = jsoniter.Unmarshal(tableInfoValue, &tableInfo)
	if err != nil {
		return nil, err
	}
	//存入缓存
	l.leveldb.TableInfos[tableName] = &tableInfo
	leveldbLogger.Infof("duyong--[GetTableInfo]数据库获取")
	return &tableInfo, nil
}

//func (l *LevelTableOpt) SetTableInfo(tableInfo *table.MyTableInfo) error {
//
//	//切换表 修改tableInfo
//	tableInfoKey := codekey.EncodeKey(common.Separator, common.TableInfoPrefix, tableInfo.TableName)
//	tableInfoValue, err := jsoniter.Marshal(tableInfo)
//	if err != nil {
//		return err
//	}
//	err = l.leveldb.Put(tableInfoKey.Bytes(), tableInfoValue)
//	if err != nil {
//		return err
//	}
//
//	//存入缓存
//
//	return nil
//}

//获取表中自增id和行号
func (l *LevelTableOpt) GetTableInfoIds(tableName string) (*table.MyTableInfoIds, error) {
	tableInfoIdsKey := codekey.EncodeKey(common.Separator, common.TableInfoIdsPrefix, tableName)
	tableInfoIdsValue, err := l.leveldb.Get(tableInfoIdsKey.Bytes())
	if err != nil && tableInfoIdsValue == nil {
		leveldbLogger.Errorf("leveldb get tableInfoIds error:%s", err)
		return nil, err
	}
	var tableInfoIds table.MyTableInfoIds
	err = jsoniter.Unmarshal(tableInfoIdsValue, &tableInfoIds)
	if err != nil {
		return nil, err
	}
	return &tableInfoIds, err
}

//设置表中自增id和行号
func (l *LevelTableOpt) SetTableInfoIds(tableName string, tableInfoIds *table.MyTableInfoIds) error {
	tableInfoIdsKey := codekey.EncodeKey(common.Separator, common.TableInfoIdsPrefix, tableName)
	tableInfoIdsValue, err := jsoniter.Marshal(tableInfoIds)
	if err != nil {
		return err
	}
	err = l.leveldb.Put(tableInfoIdsKey.Bytes(), tableInfoIdsValue)
	if err != nil {
		return err
	}
	return nil
}

func (l *LevelTableOpt) CreateTable(tableInfo *table.MyTableInfo) error {
	jsoniter := jsoniter.ConfigCompatibleWithStandardLibrary

	tableInfoKey := codekey.EncodeKey(common.Separator, common.TableInfoPrefix, tableInfo.TableName)
	//leveldbLogger.Infof("create table key:%s,value:%v", tableInfoKey.String(), tableInfo)
	tableInfoValue, err := jsoniter.Marshal(tableInfo)
	if err != nil {
		return err
	}
	err = l.leveldb.Put(tableInfoKey.Bytes(), tableInfoValue)
	if err != nil {
		return err
	}

	//缓存tableInfo
	l.leveldb.TableInfos[tableInfo.TableName] = tableInfo

	//写入table自增id和行数
	tableInfoIdsKey := codekey.EncodeKey(common.Separator, common.TableInfoIdsPrefix, tableInfo.TableName)
	tableInfoIds := &table.MyTableInfoIds{AutoIncId: 1, RowsCount: 0}
	tableInfoIdsValue, err := jsoniter.Marshal(tableInfoIds)
	if err != nil {
		return err
	}
	err = l.leveldb.Put(tableInfoIdsKey.Bytes(), tableInfoIdsValue)
	if err != nil {
		return err
	}

	//缓存tableids
	tableIdsKey := common2.TableIdsKey

	tableIdsValue, err := jsoniter.Marshal(l.leveldb.TableIds)
	if err != nil {
		return err
	}
	err = l.leveldb.Put([]byte(tableIdsKey), tableIdsValue)
	if err != nil {
		return err
	}
	l.leveldb.TableIds = append(l.leveldb.TableIds, tableInfo.TableId)

	return nil
}

func (l *LevelTableOpt) AddRecords(tableInfo *table.MyTableInfo, batchRows []table.Rows) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	jsoniter := jsoniter.ConfigCompatibleWithStandardLibrary
	//构造key 批量插入
	var keys, values [][]byte
	for _, rows := range batchRows {
		for _, row := range rows {
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix,
				strconv.FormatUint(tableInfo.TableId, 10), strconv.FormatUint(row.RowId, 10))
			rowValue, err := jsoniter.Marshal(row)
			if err != nil {
				errStr := fmt.Sprintf("marshal rowValue error(%s)", err)
				return errors.New(errStr)
			}
			keys = append(keys, b.Bytes())
			values = append(values, rowValue)
			//唯一索引数据
			for indexName, indexColumn := range tableInfo.UniqIndices {
				b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(tableInfo.TableId, 10),
					strconv.FormatUint(indexColumn.Idx, 10), row.ColumnValue[indexName])
				keys = append(keys, b.Bytes())
				values = append(values, []byte(strconv.FormatUint(row.RowId, 10)))

			}
			//普通索引数据
			for indexName, indexColumn := range tableInfo.Indices {
				b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(tableInfo.TableId, 10),
					strconv.FormatUint(indexColumn.Idx, 10), row.ColumnValue[indexName], strconv.FormatUint(row.RowId, 10))
				keys = append(keys, b.Bytes())
				values = append(values, []byte(strconv.FormatUint(row.RowId, 10)))
			}
			//tableInfo.RowsCount++
		}
	}

	tableInfoKey := codekey.EncodeKey(common.Separator, common.TableInfoPrefix, tableInfo.TableName)
	tableInfoValue, err := jsoniter.Marshal(tableInfo)
	if err != nil {
		return err
	}
	err = l.leveldb.Put(tableInfoKey.Bytes(), tableInfoValue)
	if err != nil {
		return err
	}

	err = l.leveldb.BatchPut(keys, values)
	if err != nil {
		return err
	}

	return nil

}

func (l *LevelTableOpt) GetRowByPrimaryField(tableName string, primaryKey []byte) (*table.Row, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	jsoniter := jsoniter.ConfigCompatibleWithStandardLibrary

	var row table.Row
	value, err := l.leveldb.Get([]byte(primaryKey))
	if err != nil {
		leveldbLogger.Errorf("GetRowByPrimaryField error:%s", string(value))
	}
	err = jsoniter.Unmarshal(value, &row)
	if err != nil {
		errStr := fmt.Sprintf("GetRowByPrimaryField error %s", err)
		return nil, errors.New(errStr)
	}
	return &row, nil
}

func (l *LevelTableOpt) GetRowIdByUniqueField(tableName string, uniqueKey []byte) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rowIdByte, err := l.leveldb.Get([]byte(uniqueKey))
	if rowIdByte == nil {
		return "", errors.New(string(uniqueKey) + "is not found")
	}
	return string(rowIdByte), err
}

func (l *LevelTableOpt) GetRows(tableName string, startKey, endKey []byte) (kv.RowsIterator, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rowsIter := l.leveldb.NewScanIterator([]byte(startKey), []byte(endKey))
	return rowsIter, nil

}

func (l *LevelTableOpt) DeleteRecords(tableName string, delKeys [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leveldb.BatchDelete(delKeys)
}

func (l *LevelTableOpt) ScanLimit(tableName string, limit int) []kv.Pair {
	return l.leveldb.Scan([]byte{}, []byte{}, limit)
}
