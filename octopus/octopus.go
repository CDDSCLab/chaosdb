package octopus

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/CDDSCLab/chaosdb/common/kv"
	"github.com/CDDSCLab/chaosdb/common/tableOpt"
	"github.com/CDDSCLab/chaosdb/executor"
	"github.com/CDDSCLab/chaosdb/opt/levelDB"
	"github.com/CDDSCLab/chaosdb/store/leveldb"

	"github.com/op/go-logging"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

type KVType string

const (
	LEVEL_DB KVType = "leveldb"
	TIKV_DB  KVType = "tikvdb"
	COUCH_DB KVType = "couchdb"
)

var octopusLogger = logging.MustGetLogger("chaosdb")

type Octopus struct {
	storage    kv.Storage        //kv存储接口
	tableOpt   tableOpt.TableOpt //表操作接口
	mu         sync.RWMutex
	kvHandlers map[KVType]map[string]*Octopus //kv句柄缓存
}

func NewOctopus() *Octopus {
	octopus := &Octopus{}
	handlerMap := make(map[KVType]map[string]*Octopus)
	octopus.kvHandlers = handlerMap
	return octopus
}

func (octo *Octopus) Open(kvType KVType, path, dbname string) (*Octopus, error) {

	//路径和文件名适配
	if path == "" {
		path = "./" + string(kvType)
	} else {
		path = strings.TrimRight(path, "/")
	}

	if dbname == "" {
		err := errors.New("leveldb dbname is empty")
		return nil, err
	}
	octopus, err := octo.getkvHandler(kvType, path+"/"+dbname)
	if err == nil {
		return octopus, nil
	}

	newOctopus, err := octo.CreateOctopus(kvType, path, dbname)

	if err != nil {
		return nil, err
	}

	octo.setkvHandler(kvType, path+"/"+dbname, newOctopus)

	return newOctopus, nil
}

func (octo *Octopus) setkvHandler(kvType KVType, pathName string, octopus *Octopus) {
	octo.mu.Lock()
	defer octo.mu.Unlock()
	pathMap := make(map[string]*Octopus)
	pathMap[pathName] = octopus
	octo.kvHandlers[kvType] = pathMap
	return
}

func (octo *Octopus) getkvHandler(kvType KVType, pathName string) (*Octopus, error) {
	octo.mu.Lock()
	defer octo.mu.Unlock()

	octopus, ok := octo.kvHandlers[kvType][pathName] //可以二维获取，不会空指针
	if !ok {
		return nil, errors.New("handler not found")
	}
	return octopus, nil
}

func (octo *Octopus) CreateOctopus(kvType KVType, path, dbname string) (*Octopus, error) {
	var storage kv.Storage
	var err error
	var tableOpt tableOpt.TableOpt
	switch kvType {
	case LEVEL_DB:
		storage, err = leveldb.NewLevelDB(path, dbname)
		tableOpt = levelDB.NewLevelTableOpt(storage)
		if err != nil {
			octopusLogger.Errorf("chaosdb -> NewLevelDB error(%s)", err)
			return nil, err
		}
	case TIKV_DB:
		errStr := fmt.Sprintf("%s db no suppot", kvType)
		return nil, errors.New(errStr)
	case COUCH_DB:
		errStr := fmt.Sprintf("%s db no suppot", kvType)
		return nil, errors.New(errStr)
	default:
		errStr := fmt.Sprintf("%s db no suppot", kvType)
		return nil, errors.New(errStr)
	}
	octopus := &Octopus{storage: storage, tableOpt: tableOpt}

	return octopus, nil
}

func (octo *Octopus) Parser(sql string) (ast.StmtNode, error) {
	sqlParser := parser.New()
	return sqlParser.ParseOneStmt(sql, "utf8", "utf8_bin")
}

// TODO: We need to rewrite the following function

//直接解析后执行 //
func (octo *Octopus) Exec(sql string) error {
	//s2k.mu.Lock()
	//	//defer s2k.mu.Unlock()
	sqlParser := parser.New()
	stmtNode, err := sqlParser.ParseOneStmt(sql, "utf8", "utf8_bin")
	if err != nil {
		//sql2kvLogger.Errorf("[sql2kv][Exec] ParseSql error sql:%s,error:%s", sql, err)
		return err
	}
	switch stmtNode.(type) {
	case *ast.CreateTableStmt:
		exec := executor.NewCreateTableExecutor(octo.tableOpt)
		err = exec.Exec(stmtNode.(*ast.CreateTableStmt))
		if err != nil {
			//sql2kvLogger.Errorf("CreateTable exec error(%s)", err)
			return err
		}
	case *ast.InsertStmt:
		exec := executor.NewInsertExecutor(octo.tableOpt)
		err = exec.Exec(stmtNode.(*ast.InsertStmt))
		if err != nil {
			//sql2kvLogger.Errorf("Insert exec error(%s)", err)
			return err
		}
	case *ast.DeleteStmt:
		exec := executor.NewDeleteExecutor(octo.tableOpt)
		err = exec.Exec(stmtNode.(*ast.DeleteStmt))
		if err != nil {
			//sql2kvLogger.Error("Delete exec error(%s)", err)
			return err
		}
	case *ast.UpdateStmt:
		exec := executor.NewUpdateExecutor(octo.tableOpt)
		err = exec.Exec(stmtNode.(*ast.UpdateStmt))
		if err != nil {
			//sql2kvLogger.Error("Update exec error(%s)", err)
			return err
		}
	default:
		errStr := fmt.Sprintf("sql type no support")
		//sql2kvLogger.Errorf(errStr)
		return errors.New(errStr)
	}
	return nil
}

func (octo *Octopus) Query(querySql string) (*executor.QueryResult, error) {
	//s2k.mu.Lock()
	//defer s2k.mu.Unlock()
	sqlParser := parser.New()
	stmtNode, err := sqlParser.ParseOneStmt(querySql, "utf8", "utf8_bin")
	if err != nil {
		errStr := fmt.Sprintf("ParseSql error sql:%s,error:%s", querySql, err)
		return nil, errors.New(errStr)
	}
	switch stmtNode.(type) {
	case *ast.SelectStmt:
		exec := executor.NewSelectExecutor(octo.tableOpt)
		res, err := exec.Query(stmtNode.(*ast.SelectStmt))
		if err != nil {
			//sql2kvLogger.Errorf("exec error(%s)", err)
			return nil, err
		}
		return res, nil
	default:
		errStr := fmt.Sprintf("Sql not a QuerySql,please call exec()")
		return nil, errors.New(errStr)
	}
}

func (octo *Octopus) Free() error {
	return octo.storage.Close()
}

//
func (octo *Octopus) ReadLimit(limit int) {
	exec := executor.NewSelectExecutor(octo.tableOpt)
	exec.ReadLimit("utxo_asset_transfer_1542610800000_1542614399999", limit)
}
