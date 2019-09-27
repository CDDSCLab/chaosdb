package executor

import (
	"errors"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"strconv"
	"sync"

	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/table"
)

type InsertExecutor struct {
	*BaseExecutor
	batchRows []table.Rows
	mu        sync.Mutex
}

var golballock sync.Mutex

func NewInsertExecutor(tableOpt tableOpt.TableOpt) *InsertExecutor {
	return &InsertExecutor{BaseExecutor: &BaseExecutor{
		TableOpt: tableOpt,
	}}
}

func (ie *InsertExecutor) Exec(insertStmtNode *ast.InsertStmt) error {

	tableSource := insertStmtNode.Table.TableRefs.Left.(*ast.TableSource)
	tableName := tableSource.Source.(*ast.TableName)

	//获取tableInfo 补全执行器
	if tableName.Name.L == "" {
		errStr := fmt.Sprint("parse error:tableName is nil")
		return errors.New(errStr)
	}

	//表是否存在
	ok, err := ie.TableOpt.TableExists(tableName.Name.L)
	if !ok {
		return err
	}

	//从获取表信息到更新表信息 只允许一个线程访问
	golballock.Lock()
	defer golballock.Unlock()
	//获取tableInfo
	err = ie.getTableInfo(tableName.Name.L)
	if err != nil {
		return err
	}
	//batchRows 用来尽可能的一次性批量写入键值
	ie.batchRows = make([]table.Rows, 0)
	rows := make([]*table.Row, 0)
	var rowId uint64
	var rowsCount uint64
	if insertStmtNode.Columns == nil {
		//insert into  `raw_utxo_index`  VALUES('autoid',2,'index');
		//以上省略插入字段的情况，后面的值必须按照顺序给出全部列  不管字段是有默认值还是主键自增，都不可省略--mysql
		for _, list := range insertStmtNode.Lists {
			var row table.Row
			row.ColumnValue = make(map[string]string)
			if len(list) != len(ie.TableInfo.Columns) {
				errStr := fmt.Sprintf("[executor][insertExcutor][exec] Insert value and column does not match")
				return errors.New(errStr)
			}
			for j, val := range list {
				//针对主键id处理
				if ie.TableInfo.Columns[j].Name == ie.TableInfo.PriKey.Name {
					var priId uint64

					if types.IsTypeNumeric(ie.TableInfo.PriKey.MysqlType.Tp) {
						priId = val.(*driver.ValueExpr).Datum.GetUint64()
					
					} else {
						priId, err = strconv.ParseUint(val.(*driver.ValueExpr).Datum.GetString(), 10, 64)
					}

					if err != nil {
						errStr := fmt.Sprint("conv primKey to int error")
						return errors.New(errStr)
					}
					if priId < ie.TableInfoIds.AutoIncId {
						errStr := fmt.Sprint("primaryKey id is smaller then Autoincrement ")
						return errors.New(errStr)
					}
					rowId = priId
					row.ColumnValue[ie.TableInfo.Columns[j].Name] = strconv.FormatUint(priId, 10)
					row.RowId = uint64(rowId)
					ie.TableInfoIds.AutoIncId = priId + 1 //下一条自增id

				} else {
					//其他字段处理
					row.ColumnValue[ie.TableInfo.Columns[j].Name] = val.(*driver.ValueExpr).Datum.GetString()
				}

			}
			rows = append(rows, &row)
			rowsCount = uint64(len(rows))
		}
		ie.batchRows = append(ie.batchRows, rows)
	} else {
		tmpRows := make([]*table.Row, 0)
		complementRows := make([]*table.Row, 0)
		//insert into  `raw_utxo_index` (`raw_utxo_index`.`BLOCKNUM`)  VALUES(2);
		//指定插入字段的情况  默认字段可省略 主键自增可省略 但是键值还是对应的
		for _, list := range insertStmtNode.Lists {
			var row table.Row
			row.ColumnValue = make(map[string]string)
			if len(list) != len(insertStmtNode.Columns) {
				errStr := fmt.Sprintf("Insert value and column does not match")
				return errors.New(errStr)
			}
			//取出没有省略的字段对应的值 如果有主键值 得到行号
			for j, val := range list {
				if insertStmtNode.Columns[j].Name.L == ie.TableInfo.PriKey.Name {

					priId, err := strconv.ParseUint(val.(*driver.ValueExpr).Datum.GetString(), 10, 64)
					if err != nil {
						errStr := fmt.Sprint("conv primaryKey to int error")
						return errors.New(errStr)
					}
					if priId < ie.TableInfoIds.AutoIncId {
						errStr := fmt.Sprint("primaryKey id is smaller then Autoincrement ")
						return errors.New(errStr)
					}
					rowId = priId
					row.ColumnValue[insertStmtNode.Columns[j].Name.L] = strconv.FormatUint(priId, 10)
					row.RowId = uint64(rowId)
					ie.TableInfoIds.AutoIncId = priId + 1

				} else {
					row.ColumnValue[insertStmtNode.Columns[j].Name.L] = val.(*driver.ValueExpr).Datum.GetString()
				}

			}
			tmpRows = append(tmpRows, &row)
			rowsCount = uint64(len(tmpRows))
		}
		//补全所有列值
		if len(insertStmtNode.Columns) != len(ie.TableInfo.Columns) {
			for _, row := range tmpRows { //不全的列
				for _, column := range ie.TableInfo.Columns { //全部的列
					if _, ok := row.ColumnValue[column.Name]; !ok {
						//不存在 则补全 有可能省略主键，自己生成
						if column.Name == ie.TableInfo.PriKey.Name {
							if types.IsTypeNumeric(column.MysqlType.Tp) {

								rowId = ie.TableInfoIds.AutoIncId
								row.RowId = uint64(rowId)
								row.ColumnValue[column.Name] = strconv.FormatUint(ie.TableInfoIds.AutoIncId, 10)
								ie.TableInfoIds.AutoIncId++ //下一条自增id

							} else {
								//主键省略 而且还不是整数类型。没法玩了
								errStr := fmt.Sprintf("when primary key is default assigned,it must be a int type")
								return errors.New(errStr)
							}

						} else {
							row.ColumnValue[column.Name] = ""
						}

					}
				}
				complementRows = append(complementRows, row)
			}
		}
		ie.batchRows = append(ie.batchRows, complementRows)
	}

	//更新表信息
	ie.TableInfoIds.RowsCount += rowsCount
	//fmt.Println(ie.TableInfo.RowsCount)
	err = ie.TableOpt.SetTableInfoIds(tableName.Name.String(), ie.TableInfoIds)
	if err != nil {
		return err
	}

	err = ie.TableOpt.AddRecords(ie.TableInfo, ie.batchRows)
	if err != nil {
		return err
	}

	//excutorLogger.Infof("[executor][createTable] tableInfo:%s", ie.TableInfo.String())
	return nil
}
