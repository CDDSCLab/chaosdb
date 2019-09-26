package table

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
)

//行结构
type Row struct {
	RowId       uint64            `json:"row_id"`       //行号
	ColumnValue map[string]string `json:"column_value"` //行记录
}

type Rows []*Row

//where条件结构
type Where struct {
	Opt        opcode.Op        //操作符
	LeftColumn string           //条件字段
	RightType  *types.FieldType //右值字段类型
	RightValue *types.Datum     //右值
}

//limit结构
type Limit struct {
	Offset uint64 //偏移值
	Count  uint64 //获取数量
}

//列结构
type Column struct {
	Idx       uint64           `json:"idx"`        //列的唯一id
	Name      string           `json:"name"`       //列名称
	MysqlType *types.FieldType `json:"mysql_type"` //列type属性
}

//表结构
type MyTableInfo struct {
	TableId     uint64             `json:"table_id"`     //表的唯一id
	TableName   string             `json:"table_name"`   //表名称
	Columns     []*Column          `json:"columns"`      //包含列 列的详细信息
	ColumnList  string             `json:"column_list"`  //只包含列名称 全部列名称以逗号分隔组成的字符串
	PriKey      *Column            `json:"pri_key"`      //主键列
	Indices     map[string]*Column `json:"indices"`      //普通索引列
	UniqIndices map[string]*Column `json:"uniq_indices"` //唯一索引列
	//TableInfo   *model.TableInfo
}

//表结构中的临界资源
type MyTableInfoIds struct {
	AutoIncId uint64 `json:"auto_inc_id"` //自增id
	RowsCount uint64 `json:"rows"`        //行数
}

//打印表信息 测试使用
func (t *MyTableInfo) String() string {
	if t == nil {
		return "<nil>"
	}
	ret := fmt.Sprintf("[table]ID:%d\n", t.TableId)
	ret += fmt.Sprintf("[table]name:%s\n", t.TableName)
	//ret += fmt.Sprintf("[table]AutoIncId:%d\n", t.AutoIncId)
	//ret += fmt.Sprintf("[table]RowsCount:%d\n", t.RowsCount)
	ret += fmt.Sprintf("[table]columns:\n")
	ret += t.printColumns()
	ret += fmt.Sprintf("\n")
	ret += fmt.Sprintf("[table]column list:%s\n", t.ColumnList)
	ret += fmt.Sprintf("[table]prikey:%v\n", t.PriKey)
	ret += fmt.Sprintf("[table]indices:\n")
	for k, v := range t.Indices {
		ret += fmt.Sprintf("key->%s,value->%v\n", k, v)
	}
	ret += fmt.Sprintf("[table]unique indices:\n")
	for k, v := range t.UniqIndices {
		ret += fmt.Sprintf("key->%s,value->%v\n", k, v)
	}
	return ret
}

//打印列信息
func (t *MyTableInfo) printColumns() string {
	ret := ""
	for _, col := range t.Columns {
		ret += fmt.Sprintf("%v", col)
	}
	return ret
}

//在表中查询列
func (t *MyTableInfo) FindCol(cols []*Column, name string) (*Column, error) {
	for _, col := range cols {
		if col.Name == name {
			return col, nil
		}
	}
	errstr := fmt.Sprintf("%s is not this table column", name)
	return nil, errors.New(errstr)
}

//构造ColumnList
func (t *MyTableInfo) BuildColumnList() {
	columns := make([]string, 0, len(t.Columns))
	for _, column := range t.Columns {
		columns = append(columns, column.Name)
	}
	t.ColumnList = strings.Join(columns, ",")
}

//解析ast结构中的键约束->转换为我们的表结构要使用的键约束
func (t *MyTableInfo) ParseTableConstraint(cons *ast.Constraint) error {
	//判断类型
	switch cons.Tp {
	//主键约束
	case ast.ConstraintPrimaryKey:
		for _, indexCol := range cons.Keys {
			name := indexCol.Column.Name.L
			column, err := t.FindCol(t.Columns, name)
			if err != nil {
				return err
			}
			t.PriKey = column
		}
		return nil
	//唯一约束
	case ast.ConstraintKey, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		for _, indexCol := range cons.Keys {
			name := indexCol.Column.Name.L
			column, err := t.FindCol(t.Columns, name)
			if err != nil {
				return err
			}
			t.UniqIndices[name] = column
		}
		return nil
	//普通索引约束
	case ast.ConstraintIndex:
		for _, indexCol := range cons.Keys {
			name := indexCol.Column.Name.L
			column, err := t.FindCol(t.Columns, name)
			if err != nil {
				return err
			}
			t.Indices[name] = column
		}
		return nil
	default:
		err := errors.New("constraint is not support")
		return err
	}
}
