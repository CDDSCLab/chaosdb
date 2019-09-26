package executor

import (
	"chaosdb/common/kv"
	"chaosdb/common/tableOpt"
	"chaosdb/opt/common"
	"chaosdb/table"
	"chaosdb/util/codekey"
	"errors"
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/op/go-logging"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
)

var excutorLogger = logging.MustGetLogger("executor")

type BaseExecutor struct {
	TableInfo    *table.MyTableInfo
	TableInfoIds *table.MyTableInfoIds
	TableOpt     tableOpt.TableOpt
}

func (be *BaseExecutor) getQueryResultWithoutWhere(selectField []string, limit *table.Limit) (*QueryResult, error) {
	var queryRes QueryResult
	//没有条件 获取所有表数据 范围查询
	sb := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10), "1")
	eb := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10), strconv.FormatUint(be.TableInfoIds.AutoIncId, 10))
	rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, sb.Bytes(), eb.Bytes())
	if err != nil {
		errStr := fmt.Sprintf("GetRows iterator error:%s", err)
		return nil, errors.New(errStr)
	}
	//sbseek := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatInt(be.TableInfo.TableId, 10), strconv.FormatInt(1+limit.Offset, 10))

	//rowIter.Seek(sbseek.Bytes())
	queryRes.isPriKey = true
	queryRes.pointSelect = false
	queryRes.rowsIterator = rowIter
	if limit.Count != 0 {
		queryRes.returnCount = limit.Count
	}

	queryRes.columnList = selectField
	queryRes.be = be
	return &queryRes, nil
}

func (be *BaseExecutor) getQueryResultWithWhere(selectFiled []string, where *table.Where, limit *table.Limit) (*QueryResult, error) {

	//条件字段合法性 :必须是索引列(1.存在 2.是索引)
	_, isUniqColumn := be.TableInfo.UniqIndices[where.LeftColumn]
	isPrimaryColumn := be.TableInfo.PriKey.Name == where.LeftColumn
	_, isIndexColumn := be.TableInfo.Indices[where.LeftColumn]
	if !isUniqColumn && !isPrimaryColumn && !isIndexColumn {
		errStr := fmt.Sprintf("The where field(%s) must be a index Column", where.LeftColumn)
		return nil, errors.New(errStr)
	}

	var queryRes QueryResult
	switch where.Opt {
	case opcode.EQ:
		//主键-单点
		if isPrimaryColumn {
			if !types.IsTypeFloat(where.RightType.Tp) {
				errStr := fmt.Sprintf("The where of primary field(%s) must be a numeric type", where.LeftColumn)
				return nil, errors.New(errStr)
			}
			rightValue := strconv.FormatInt(where.RightValue.GetInt64(), 10)
			//构造主键key tableId_whereColumnValueId(rowId)
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				rightValue)
			row, err := be.TableOpt.GetRowByPrimaryField(be.TableInfo.TableName, b.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("GetRowByPrimaryField error,key:%s", b.String())
				return nil, errors.New(errStr)
			}
			queryRes.isPriKey = true
			queryRes.pointSelect = true
			queryRes.row = row
			//唯一索引 单点
		} else if isUniqColumn {
			var rightValue string
			if types.IsTypeFloat(where.RightType.Tp) {
				rightValue = strconv.FormatInt(where.RightValue.GetInt64(), 10)
			} else if types.IsTypeChar(where.RightType.Tp) {
				rightValue = where.RightValue.GetString()
			}
			//构造唯一key tableId_whereColumnId_whereColumnValue
			ub := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.UniqIndices[where.LeftColumn].Idx, 10),
				rightValue)
			rowId, err := be.TableOpt.GetRowIdByUniqueField(be.TableInfo.TableName, ub.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("GetRowIdByUniqueField error,key:%s", ub.String())
				return nil, errors.New(errStr)
			}

			pb := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10), rowId)
			row, err := be.TableOpt.GetRowByPrimaryField(be.TableInfo.TableName, pb.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("GetRowByUniqueField error,key:%s", pb.String())
				return nil, errors.New(errStr)
			}
			queryRes.isPriKey = false
			queryRes.pointSelect = true
			queryRes.row = row
			//普通索引 范围
		} else if isIndexColumn {
			var rightValue string
			//取出条件右值
			if types.IsTypeFloat(where.RightType.Tp) {
				rightValue = strconv.FormatInt(where.RightValue.GetInt64(), 10)

			} else if types.IsTypeChar(where.RightType.Tp) {
				rightValue = where.RightValue.GetString()

			}
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.Indices[where.LeftColumn].Idx, 10),
				rightValue)

			start := b.String() + "_1"
			//fmt.Println("startKey:", start)

			//只给startKey 我们除了满足条件的前缀key以外，迭代器中还会有全部其他的key
			//最好的方式是给出endKey 这样会只有符合条件的key 但是rocksdb 在结果集调用next方式时，这个迭代器会无效....
			//现在的处理办法，在next中判断前缀，如果出现不同的前缀，则关闭此迭代器
			//end := ""
			//end := b.String() + "_" + strconv.FormatInt(be.TableInfo.RowsCount+1, 10)
			//fmt.Println("endKey:", end)
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, []byte(start), nil)

			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}

			//for ; rowIter.ValidForPrefix(b.Bytes()); rowIter.Next() {
			//	fmt.Println("key:", string(rowIter.Key()))
			//}
			queryRes.validPrefix = b.String()
			queryRes.isPriKey = false
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		}
	case opcode.GT:
		//右值必须是是数字
		if !types.IsTypeFloat(where.RightType.Tp) {
			errStr := fmt.Sprint("The where of '>','<' condition must be a  numeric type")
			return nil, errors.New(errStr)
		}
		rightValue := where.RightValue.GetUint64()
		if isPrimaryColumn {
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, b.Bytes(), []byte(""))
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			bseek := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(rightValue+limit.Offset, 10))
			rowIter.Seek(bseek.Bytes())
			if limit.Offset == 0 {
				rowIter.Next()
			}
			queryRes.isPriKey = true
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		} else if isUniqColumn {
			ub := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.UniqIndices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, ub.Bytes(), nil)
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			ubseek := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.UniqIndices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue+limit.Offset, 10))
			rowIter.Seek(ubseek.Bytes())
			if limit.Offset == 0 {
				rowIter.Next()
			}
			queryRes.isPriKey = false
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		} else if isIndexColumn {
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.Indices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, b.Bytes(), nil)
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			bseek := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.Indices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue+limit.Offset, 10))
			rowIter.Seek(bseek.Bytes())
			if limit.Offset == 0 {
				rowIter.Next()
			}
			queryRes.isPriKey = false
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		}
	case opcode.LT:
		if !types.IsTypeFloat(where.RightType.Tp) {
			errStr := fmt.Sprint("The where of '>','<' condition must be a  numeric type")
			return nil, errors.New(errStr)
		}
		rightValue := where.RightValue.GetUint64()
		if isPrimaryColumn {
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, []byte{}, b.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			if limit.Offset != 0 {
				bseek := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
					strconv.FormatUint(limit.Offset, 10))
				rowIter.Seek(bseek.Bytes())
			}
			queryRes.isPriKey = true
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		} else if isUniqColumn {
			ub := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.UniqIndices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, []byte{}, ub.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			queryRes.isPriKey = false
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		} else if isIndexColumn {
			b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.IndexPrefix, strconv.FormatUint(be.TableInfo.TableId, 10),
				strconv.FormatUint(be.TableInfo.Indices[where.LeftColumn].Idx, 10),
				strconv.FormatUint(rightValue, 10))
			rowIter, err := be.TableOpt.GetRows(be.TableInfo.TableName, []byte{}, b.Bytes())
			if err != nil {
				errStr := fmt.Sprintf("Useing where Condition:%s GetRows iterator error:%s", opcode.EQ.String(), err)
				return nil, errors.New(errStr)
			}
			rowIter.Next()
			queryRes.isPriKey = false
			queryRes.pointSelect = false
			queryRes.rowsIterator = rowIter
			if limit.Count != 0 {
				queryRes.returnCount = limit.Count
			}
		}
	case opcode.NE:
		errStr := fmt.Sprintf("Useing where Condition:%s is no support", opcode.NE.String())
		return nil, errors.New(errStr)
	case opcode.GE:
		errStr := fmt.Sprintf("Useing where Condition:%s is no support", opcode.GE.String())
		return nil, errors.New(errStr)
	case opcode.LE:
		errStr := fmt.Sprintf("Useing where Condition:%s is no support", opcode.LE.String())
		return nil, errors.New(errStr)
	default:
		errStr := fmt.Sprintf("Useing where Condition:%s is no support", opcode.EQ.String())
		return nil, errors.New(errStr)
	}
	queryRes.be = be
	queryRes.columnList = selectFiled
	return &queryRes, nil
}

func (be *BaseExecutor) getTableInfo(tableName string) error {
	//表是否存在
	ok, err := be.TableOpt.TableExists(tableName)
	if !ok {
		return err
	}
	//获取tableInfo 补全执行器
	if tableName == "" {
		errStr := fmt.Sprint("parse error:tableName is nil")
		return errors.New(errStr)
	}

	tableInfo, err := be.TableOpt.GetTableInfo(tableName)

	if err != nil {
		errStr := fmt.Sprintf("get tableinfo error(%s)", err)
		return errors.New(errStr)
	}
	be.TableInfo = tableInfo

	tableInfoIds, err := be.TableOpt.GetTableInfoIds(tableName)
	if err != nil {
		errStr := fmt.Sprintf("get tableinfoIds error(%s)", err)
		return errors.New(errStr)
	}
	be.TableInfoIds = tableInfoIds
	return nil
}

type QueryResult struct {
	isPriKey     bool            //是否为主键查询  主键查询可直接拼接key
	rowsIterator kv.RowsIterator //对外行迭代器
	pointSelect  bool            //点查结果集 Or 范围查结果集
	row          *table.Row      //点查结果
	returnCount  uint64          //对外返回结果集中合法数据总条数
	hasReturn    uint64          //已经返回的合法数据条数
	columnList   []string        //选择列
	be           *BaseExecutor   //
	validPrefix  string          //键前缀
}

func (qr *QueryResult) Next(row *table.Row) bool {

	if qr.pointSelect {
		tmpRow, err := qr.GetRow()
		if err != nil {
			errStr := fmt.Sprintf("get row error:%s", err)
			excutorLogger.Errorf(errStr)
			return false
		}
		if qr.hasReturn == 1 {
			return false
		} else {
			*row = *tmpRow
			qr.hasReturn++
			return true
		}
	}
	row.ColumnValue = make(map[string]string)

	if !qr.rowsIterator.Valid() {
		qr.rowsIterator.Close()
		return false
	}
	if qr.validPrefix != "" {
		if !qr.rowsIterator.ValidForPrefix([]byte(qr.validPrefix)) {
			qr.rowsIterator.Close()
			return false
		}
	}

	if (qr.returnCount > 0) && (qr.hasReturn >= qr.returnCount) {
		qr.rowsIterator.Close()
		return false
	}

	//范围查询 获取行id

	if qr.isPriKey {
		//如果查询条件是主键 直接通过迭代器value获取行信息
		jsoniter := jsoniter.ConfigCompatibleWithStandardLibrary
		var tmp table.Row
		jsoniter.Unmarshal(qr.rowsIterator.Value(), &tmp)
		row.RowId = tmp.RowId
		for _, column := range qr.columnList {
			row.ColumnValue[column] = tmp.ColumnValue[column]
		}
	} else {
		//唯一索引 值为rowid
		rowid := string(qr.rowsIterator.Value())

		//普通索引 键中取出rowid
		if rowid == "" {
			keySlice := strings.Split(string(qr.rowsIterator.Key()), common.Separator)
			rowid = keySlice[len(keySlice)-1]
		}

		//拼接行信息键 主键id键
		b := codekey.EncodeKey(common.Separator, common.TablePrefix, common.RowPrefix, strconv.FormatUint(qr.be.TableInfo.TableId, 10), rowid)

		//for ; qr.rowsIterator.Valid(); qr.rowsIterator.Next() {
		//	kk := string(qr.rowsIterator.Key())
		//	vv := string(qr.rowsIterator.Value())
		//	fmt.Println("k:", kk, ",v:", vv)
		//}
		//获取行信息
		rowtmp, err := qr.be.TableOpt.GetRowByPrimaryField(qr.be.TableInfo.TableName, b.Bytes())

		if err != nil {
			errStr := fmt.Sprintf("GetRowByPrimaryField error,key:%s", b.String())
			excutorLogger.Errorf(errStr)
			qr.rowsIterator.Close()
			return false
		}

		row.RowId = rowtmp.RowId
		//过滤字段数据
		for _, column := range qr.columnList {
			row.ColumnValue[column] = rowtmp.ColumnValue[column]
		}
	}

	qr.hasReturn++
	qr.rowsIterator.Next()

	return true
}

func (qr *QueryResult) GetRow() (*table.Row, error) {

	if qr.pointSelect {
		//返回对应列
		row := table.Row{}
		row.ColumnValue = make(map[string]string)
		row.RowId = qr.row.RowId
		for _, cloumn := range qr.columnList {
			row.ColumnValue[cloumn] = qr.row.ColumnValue[cloumn]
		}
		return &row, nil
	} else {
		errStr := fmt.Sprintf("multi-rows call QueryResult.Next to get row")
		return nil, errors.New(errStr)
	}
}
