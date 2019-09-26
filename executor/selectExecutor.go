package executor

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	"github.com/pkg/errors"

	"strings"

	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/table"
)

type SelectExecutor struct {
	*BaseExecutor
	selectField []string
	where       *table.Where
	limit       *table.Limit
}

func NewSelectExecutor(tableOpt tableOpt.TableOpt) *SelectExecutor {

	return &SelectExecutor{BaseExecutor: &BaseExecutor{
		TableOpt: tableOpt,
	}}
}

func (se *SelectExecutor) Query(selectStmtNode *ast.SelectStmt) (*QueryResult, error) {
	tableSource := selectStmtNode.From.TableRefs.Left.(*ast.TableSource)
	tableName := tableSource.Source.(*ast.TableName)
	//获取tableInfo
	if tableName.Name.L == "" {
		errStr := fmt.Sprint("parse QuerySql error: tableName is nil")
		return nil, errors.New(errStr)

	}
	err := se.getTableInfo(tableName.Name.L)
	if err != nil {
		return nil, err
	}
	//获取查询字段
	se.selectField = make([]string, 0)
	firstField := selectStmtNode.Fields.Fields[0]
	if len(selectStmtNode.Fields.Fields) == 1 && firstField.Expr == nil && firstField.Text() == "" {
		se.selectField = strings.Split(se.TableInfo.ColumnList, ",")
	} else {
		for _, field := range selectStmtNode.Fields.Fields {
			se.selectField = append(se.selectField, strings.ToLower(field.Text()))
		}
	}

	//查询字段合法性验证
	for _, field := range se.selectField {
		_, err := se.TableInfo.FindCol(se.TableInfo.Columns, field)
		if err != nil {
			return nil, err
		}
	}

	//limit获取
	limit := &table.Limit{}
	if selectStmtNode.Limit != nil {
		limit.Offset = selectStmtNode.Limit.Offset.GetDatum().GetUint64()
		limit.Count = selectStmtNode.Limit.Count.GetDatum().GetUint64()
	}
	se.limit = limit

	if selectStmtNode.Where == nil {

		return se.getQueryResultWithoutWhere(se.selectField, se.limit)
	}

	//带where条件
	operationExpr := selectStmtNode.Where.(*ast.BinaryOperationExpr)
	where := &table.Where{}
	//只支持单个where，where表达式操作符只包含以下操作 >= <= = != > <
	if operationExpr.Op == opcode.GE || operationExpr.Op == opcode.LE || operationExpr.Op == opcode.EQ ||
		operationExpr.Op == opcode.NE || operationExpr.Op == opcode.LT || operationExpr.Op == opcode.GT {
		where.Opt = operationExpr.Op
		where.LeftColumn = operationExpr.L.(*ast.ColumnNameExpr).Name.String()
		where.RightType = operationExpr.R.GetType()
		where.RightValue = operationExpr.R.GetDatum()
		se.where = where
	} else {
		errStr := fmt.Sprintf("no support %s where operator", operationExpr.Op.String())
		return nil, errors.New(errStr)
	}

	return se.getQueryResultWithWhere(se.selectField, se.where, se.limit)
}

func (se *SelectExecutor) ReadLimit(tableName string, limit int) {

	kvs := se.TableOpt.ScanLimit(tableName, limit)
	for _, kv := range kvs {
		kk := string(kv.Key)
		vv := string(kv.Value)
		fmt.Println("key:", kk, ",value:", vv)
	}
}
