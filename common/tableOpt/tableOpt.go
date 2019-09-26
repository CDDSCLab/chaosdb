package tableOpt

import (
	"github.com/CDDSCLab/chaosdb/common/kv"
	"github.com/CDDSCLab/chaosdb/table"
)

type TableOpt interface {
	//表是否存在
	TableExists(tableName string) (bool, error)
	//获取唯一表id
	GetUniqTableId() uint64
	//根据表名获取表信息
	GetTableInfo(tableName string) (*table.MyTableInfo, error)
	//设置表信息
	//SetTableInfo(tableInfo *table.MyTableInfo) error
	//获取表中自增id和行号
	GetTableInfoIds(tableName string) (*table.MyTableInfoIds, error)
	//设置表中自增id和行号
	SetTableInfoIds(tableName string, tableInfoIds *table.MyTableInfoIds) error
	//创建表
	CreateTable(tableInfo *table.MyTableInfo) error
	//新增记录
	AddRecords(tableInfo *table.MyTableInfo, rows []table.Rows) error
	//根据主键字段获取行信息
	GetRowByPrimaryField(tableName string, primaryKey []byte) (*table.Row, error)
	//根据唯一索引字段行信息
	GetRowIdByUniqueField(tableName string, uniqueKey []byte) (string, error)
	//获取范围内的行
	GetRows(tableName string, startKey, endKey []byte) (kv.RowsIterator, error)
	//删除记录
	DeleteRecords(tableName string, delKeys [][]byte) error
	//获取全部记录--测试查看数据时使用
	ScanLimit(tableName string, limit int) []kv.Pair
}
