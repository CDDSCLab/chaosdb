package executor

import (
	"errors"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"

	"chaosdb/common/tableOpt"
	"chaosdb/opt/common"
	"chaosdb/table"
	"chaosdb/util/codekey"
	"strconv"
	"strings"
)

type DeleteExecutor struct {
	*BaseExecutor
	where *table.Where
	limit *table.Limit
}

func NewDeleteExecutor(tableOpt tableOpt.TableOpt) *DeleteExecutor {
	return &DeleteExecutor{BaseExecutor: &BaseExecutor{
		TableOpt: tableOpt,
	}}
}

func (de *DeleteExecutor) Exec(deleteStmtNode *ast.DeleteStmt) error {
	tableSource := deleteStmtNode.TableRefs.TableRefs.Left.(*ast.TableSource)
	tableName := tableSource.Source.(*ast.TableName)
	err := de.getTableInfo(tableName.Name.L)
	if err != nil {
		return err
	}
	//limit获取
	limit := &table.Limit{}
	if deleteStmtNode.Limit != nil {
		limit.Offset = deleteStmtNode.Limit.Offset.GetDatum().GetUint64()
		limit.Count = deleteStmtNode.Limit.Count.GetDatum().GetUint64()
	}
	de.limit = limit

	//delete必须有条件
	if deleteStmtNode.Where == nil {
		errStr := fmt.Sprintf("delete must hava a where condition")
		return errors.New(errStr)
	}

	//带where条件判断 后续封装
	operationExpr := deleteStmtNode.Where.(*ast.BinaryOperationExpr)
	where := &table.Where{}
	//只支持单个where，where表达式操作符只包含以下操作 >= <= = != > <
	if operationExpr.Op == opcode.GE || operationExpr.Op == opcode.LE || operationExpr.Op == opcode.EQ ||
		operationExpr.Op == opcode.NE || operationExpr.Op == opcode.LT || operationExpr.Op == opcode.GT {
		where.Opt = operationExpr.Op
		where.LeftColumn = operationExpr.L.(*ast.ColumnNameExpr).Name.String()
		where.RightType = operationExpr.R.GetType()
		where.RightValue = operationExpr.R.GetDatum()
		de.where = where
	} else {
		errStr := fmt.Sprintf("no support %s where operator", operationExpr.Op.String())
		return errors.New(errStr)
	}

	//获取记录
	selectField := make([]string, 0)
	selectField = strings.Split(de.TableInfo.ColumnList, ",")
	queryRes, err := de.getQueryResultWithWhere(selectField, de.where, de.limit)
	if err != nil {
		excutorLogger.Errorf("get records error when delete:%s", err)
		return err
	}
	var row table.Row
	var keys = make([][]byte, 0)
	//后续得到结果集条数，开线程处理删除
	for queryRes.Next(&row) {
		excutorLogger.Infof("row:%v\n", row)
		if de.TableInfo.PriKey != nil {
			priKey := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix,
				strconv.FormatUint(de.TableInfo.TableId, 10), strconv.FormatUint(row.RowId, 10))
			keys = append(keys, priKey.Bytes())
		}
		if de.TableInfo.UniqIndices != nil {
			for uniqName, uniqColumn := range de.TableInfo.UniqIndices {
				uniqKey := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix,
					strconv.FormatUint(de.TableInfo.TableId, 10), strconv.FormatUint(uniqColumn.Idx, 10),
					row.ColumnValue[uniqName])
				keys = append(keys, uniqKey.Bytes())
			}
		}
		if de.TableInfo.Indices != nil {
			for indexName, indexColumn := range de.TableInfo.Indices {
				indexKey := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(de.TableInfo.TableId, 10),
					strconv.FormatUint(indexColumn.Idx, 10), row.ColumnValue[indexName], strconv.FormatUint(row.RowId, 10))
				keys = append(keys, indexKey.Bytes())
			}
		}

		//for _, key := range keys {
		//	excutorLogger.Infof("delete key:%s\n", string(key))
		//}
		//批量删除键
		de.TableOpt.DeleteRecords(de.TableInfo.TableName, keys)
	}
	return nil

}
