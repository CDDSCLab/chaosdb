package executor

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
	"github.com/pkg/errors"

	"strconv"
	"strings"

	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/opt/common"
	"github.com/CDDSCLab/chaosdb/table"
	"github.com/CDDSCLab/chaosdb/util/codekey"
)

type Assign struct {
	ColumnName string
	AssignType *types.FieldType
	AssignVale *types.Datum
	IsIndex    bool
	IsUnique   bool
	IndexId    uint64
}

type UpdateExecutor struct {
	*BaseExecutor
	lists []Assign
	where *table.Where
	limit *table.Limit
}

func NewUpdateExecutor(tableOpt tableOpt.TableOpt) *UpdateExecutor {
	return &UpdateExecutor{BaseExecutor: &BaseExecutor{
		TableOpt: tableOpt,
	}}
}

func (ue *UpdateExecutor) Exec(updateStmtNode *ast.UpdateStmt) error {
	tableSource := updateStmtNode.TableRefs.TableRefs.Left.(*ast.TableSource)
	tableName := tableSource.Source.(*ast.TableName)
	err := ue.getTableInfo(tableName.Name.L)
	if err != nil {
		return err
	}

	//update 必须有条件，避免全表更新
	if updateStmtNode.Where == nil {
		errStr := fmt.Sprintf("delete must hava a where condition")
		return errors.New(errStr)
	}

	//limit获取
	limit := &table.Limit{}
	if updateStmtNode.Limit != nil {
		limit.Offset = updateStmtNode.Limit.Offset.GetDatum().GetUint64()
		limit.Count = updateStmtNode.Limit.Count.GetDatum().GetUint64()
	}
	ue.limit = limit

	//获取更新字段
	ue.lists = make([]Assign, 0)
	for _, assignment := range updateStmtNode.List {
		var assign Assign
		//不可更新主键字段
		if assignment.Column.Name.L == ue.TableInfo.PriKey.Name {
			errStr := fmt.Sprintf("Primary field can not update")
			return errors.New(errStr)
		}
		if uniqColumn, ok := ue.TableInfo.UniqIndices[assignment.Column.Name.L]; ok {
			assign.IsUnique = true
			assign.IndexId = uniqColumn.Idx
		}
		if indexColumn, ok := ue.TableInfo.Indices[assignment.Column.Name.L]; ok {
			assign.IsIndex = true
			assign.IndexId = indexColumn.Idx
		}
		assign.ColumnName = assignment.Column.Name.L
		assign.AssignType = assignment.Expr.GetType()
		assign.AssignVale = assignment.Expr.GetDatum()
		ue.lists = append(ue.lists, assign)
	}

	operationExpr := updateStmtNode.Where.(*ast.BinaryOperationExpr)
	where := &table.Where{}
	//只支持单个where，where表达式操作符只包含以下操作 >= <= = != > <
	if operationExpr.Op == opcode.GE || operationExpr.Op == opcode.LE || operationExpr.Op == opcode.EQ ||
		operationExpr.Op == opcode.NE || operationExpr.Op == opcode.LT || operationExpr.Op == opcode.GT {
		where.Opt = operationExpr.Op
		where.LeftColumn = operationExpr.L.(*ast.ColumnNameExpr).Name.String()
		where.RightType = operationExpr.R.GetType()
		where.RightValue = operationExpr.R.GetDatum()
		ue.where = where
	} else {
		errStr := fmt.Sprintf("no support %s where operator", operationExpr.Op.String())
		return errors.New(errStr)
	}
	selectField := make([]string, 0)
	selectField = strings.Split(ue.TableInfo.ColumnList, ",")
	queryRes, err := ue.getQueryResultWithWhere(selectField, ue.where, ue.limit)
	if err != nil {
		excutorLogger.Errorf("get records error when delete:%s", err)
		return err
	}
	var row table.Row
	for queryRes.Next(&row) {
		var addRows = make(table.Rows, 0)
		var deleteKeys = make([][]byte, 0)
		var batchRows = make([]table.Rows, 0)
		//excutorLogger.Infof("row:%v\n", row)
		//根据表中 唯一索引和普通索引的字段，收集需要删除的字段
		for _, list := range ue.lists {
			if list.IsUnique {
				//是唯一索引，删除索引键
				uniqKey := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix,
					strconv.FormatUint(ue.TableInfo.TableId, 10), strconv.FormatUint(list.IndexId, 10),
					row.ColumnValue[list.ColumnName])
				deleteKeys = append(deleteKeys, uniqKey.Bytes())
			}
			if list.IsIndex {
				indexKey := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(ue.TableInfo.TableId, 10),
					strconv.FormatUint(list.IndexId, 10), row.ColumnValue[list.ColumnName], strconv.FormatUint(row.RowId, 10))
				deleteKeys = append(deleteKeys, indexKey.Bytes())
			}
			//字段不是索引 直接更新行数据
			row.ColumnValue[list.ColumnName] = list.AssignVale.GetString()
		}
		addRows = append(addRows, &row)
		//for _, row := range addRows {
		//	fmt.Println(row)
		//}
		//for _, key := range deleteKeys {
		//	fmt.Println(string(key))
		//}
		batchRows = append(batchRows, addRows)
		err := ue.TableOpt.AddRecords(ue.TableInfo, batchRows)
		if err != nil {
			excutorLogger.Errorf("set NewRecords errror", err)
			return err
		}
		err = ue.TableOpt.DeleteRecords(ue.TableInfo.TableName, deleteKeys)
		if err != nil {
			excutorLogger.Errorf("delete Records errror", err)
			return err
		}
	}

	return nil
}
