package executor

import (
	"errors"
	"fmt"

	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/table"

	"github.com/pingcap/parser/ast"
)

type CreateTableExecutor struct {
	*BaseExecutor
	IfNotExists bool
}

//新建执行器
func NewCreateTableExecutor(tableOpt tableOpt.TableOpt) *CreateTableExecutor {

	tableInfo := &table.MyTableInfo{
		Indices:     make(map[string]*table.Column),
		UniqIndices: make(map[string]*table.Column),
	}
	return &CreateTableExecutor{BaseExecutor: &BaseExecutor{
		TableOpt:  tableOpt,
		TableInfo: tableInfo,
	}}
}

func (ce *CreateTableExecutor) Exec(createStmtNode *ast.CreateTableStmt) error {
	//检测表是否已经创建
	//标志
	ce.IfNotExists = createStmtNode.IfNotExists
	//表名
	ce.TableInfo.TableName = createStmtNode.Table.Name.String()

	//检测表是否已创建
	ok, err := ce.TableOpt.TableExists(ce.TableInfo.TableName)
	if ok {
		if !ce.IfNotExists {
			errStr := fmt.Sprintf("[executor][parseAst2TableInfo] table(%s) is exists", ce.TableInfo.TableName)
			return errors.New(errStr)
		}
		return nil
	}
	//解析ast
	err = ce.parseAst2TableInfo(createStmtNode)
	if err != nil {
		return err
	}
	//执行操作
	ce.TableOpt.CreateTable(ce.TableInfo)
	return nil
}

func (ce *CreateTableExecutor) parseAst2TableInfo(stmt *ast.CreateTableStmt) error {
	//获取唯一TableID
	tableID := ce.TableOpt.GetUniqTableId()
	//tableInfo构建
	ce.TableInfo.TableId = tableID
	ce.TableInfo.Columns = make([]*table.Column, 0, len(stmt.Cols))

	for i, col := range stmt.Cols {
		column := &table.Column{Idx: uint64(i) + 1, Name: col.Name.Name.L, MysqlType: col.Tp}

		ce.TableInfo.Columns = append(ce.TableInfo.Columns, column)
	}
	for _, cons := range stmt.Constraints {
		err := ce.TableInfo.ParseTableConstraint(cons)
		if err != nil {
			return err
		}
	}
	ce.TableInfo.BuildColumnList()
	//excutorLogger.Infof("[executor][createTable] tableInfo:%s", ce.TableInfo.String())
	return nil
}
