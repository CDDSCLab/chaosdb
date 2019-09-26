package main

import (
	"fmt"

	"github.com/CDDSCLab/chaosdb/octopus"
	"github.com/CDDSCLab/chaosdb/table"

	"github.com/CDDSCLab/chaosdb/executor"
)

func main() {

	octoHandler := octopus.NewOctopus()
	fmt.Println(octoHandler)
	fmt.Printf("---实例化句柄---")

	sql2kvRocksdb, err := octoHandler.Open(octopus.LEVEL_DB, "./leveldb", "simple_leveldb_data")
	if err != nil {
		panic(err)
	}

	sql2kvRocksdb, err = octoHandler.Open(octopus.LEVEL_DB, "./leveldb", "simple_leveldb_data")
	if err != nil {
		panic(err)
	}

	fmt.Println("--实例化句柄成功---")

	fmt.Println("--创建表操作---")
	createTable(sql2kvRocksdb)
	fmt.Println("--创建表成功---")

	fmt.Println("--插入操作---")
	rocksdbBatchInsert(sql2kvRocksdb)
	fmt.Println("--插入操作结束---")

	fmt.Println("--根据主键查询一条(where id=5)---")
	selectFieldsWithPrimaryCondition(sql2kvRocksdb)
	fmt.Println("--根据主键查询一条结束---")

	fmt.Println("--根据主键查询多条(where id>80 limit 0,10)---")
	selectAllFieldWithPrimaryCinditionRange(sql2kvRocksdb)
	fmt.Println("--根据主键查询多条结束---")

	fmt.Println("--普通索引查询---")
	selectRange(sql2kvRocksdb)
	fmt.Println("--普通索引查询结束---")

}

func createTable(sql2kvRocksdb *octopus.Octopus) {
	createSql := `CREATE TABLE IF NOT EXISTS utxo_asset_transfer_1542610800000_1542614399999(
  ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PUBLICID varchar(1024) COLLATE utf8_bin NOT NULL,
  PUBLICHASH char(64) COLLATE utf8_bin NOT NULL,
  TXTIME bigint(20) unsigned NOT NULL,
  TXID char(64) COLLATE utf8_bin NOT NULL,
  HASHINDEX int(10) unsigned NOT NULL,
  TXTYPE smallint(6) NOT NULL,
  AMOUNT bigint(20) NOT NULL,
  REMARK varchar(256) COLLATE utf8_bin NOT NULL,
  HASH char(64) COLLATE utf8_bin NOT NULL,
  PUBLICTYPE tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (ID),
  KEY PUBLICHASH (PUBLICHASH,TXTIME,TXTYPE),
  KEY TXTIME (TXTIME),
  KEY HASH (HASH,HASHINDEX,TXTYPE)
) ENGINE=MyISAM AUTO_INCREMENT=1117209 DEFAULT CHARSET=utf8 COLLATE=utf8_bin`
	err := sql2kvRocksdb.Exec(createSql)
	if err != nil {
		panic(err)
	}

}

func rocksdbBatchInsert(sql2kvRocksdb *octopus.Octopus) {

	//withField
	insertSql := "insert into `utxo_asset_transfer_1542610800000_1542614399999` ( `PUBLICID`, `PUBLICHASH`, `TXTIME`, `TXID`, `HASHINDEX`, `TXTYPE`, `AMOUNT`, `REMARK`, `HASH`, `PUBLICTYPE`) values"
	for i := 0; i < 5; i++ {
		insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b713df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '1', '100', 'yC', '04dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7'),"
	}
	for i := 0; i < 5; i++ {
		insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b813df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '1', '100', 'yC', '04dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7'),"
	}
	for i := 0; i < 5; i++ {
		insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b713df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '4', '100', 'yC', '04dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7'),"
	}
	for i := 0; i < 5; i++ {
		insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b713df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '1', '100', 'yC', '05dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7'),"
	}
	for i := 0; i < 5; i++ {
		insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b813df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '1', '100', 'yC', '04dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7'),"
	}
	insertSql += " ('c7e359653648e3576ad8a0bb8683461f611c5aa8ba71db7adc7ad9514c62e9c0', 'b713df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e', '1542614204182', '3a49b0b4db434d5d8a00ea4e9a4920d3', '0', '1', '100', 'yC', '04dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f', '7')"

	err := sql2kvRocksdb.Exec(insertSql)
	if err != nil {
		panic(err)
	}
	fmt.Println("ok...")

}

func selectFieldsWithPrimaryCondition(sql2kvRocksdb *octopus.Octopus) {
	fmt.Println("-------------selectFieldsWithPrimaryCondition--------------------")
	sql := "select * from utxo_asset_transfer_1542610800000_1542614399999 where ID=5"
	var err error
	var res *executor.QueryResult
	res, err = sql2kvRocksdb.Query(sql)
	if err != nil {
		panic(err)
	}
	row, err := res.GetRow()
	if err != nil {
		panic(err)
	}
	for rowName, rowValue := range row.ColumnValue {
		name := rowName
		value := rowValue
		fmt.Println("name:", name, ",value:", value)
	}
}

func selectAllFieldWithPrimaryCinditionRange(sql2kvRocksdb *octopus.Octopus) {
	fmt.Println("-------------selectAllFieldWithPrimaryCinditionRange >  --------------------")
	var err error
	var res *executor.QueryResult
	sql := "select * from utxo_asset_transfer_1542610800000_1542614399999 where ID>80 LIMIT 0,10"

	res, err = sql2kvRocksdb.Query(sql)

	if err != nil {
		panic(err)
	}
	var row table.Row

	for res.Next(&row) {
		fmt.Println("rowid:", row.RowId)
		for rowName, rowValue := range row.ColumnValue {
			name := rowName
			value := rowValue
			fmt.Println("name:", name, ",value:", value)
		}
	}
}

func selectRange(sql2kvRocksdb *octopus.Octopus) {
	var err error
	var res *executor.QueryResult
	var sql string
	var row table.Row
	fmt.Println("---------------------------------")
	//b713df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e

	sql = "select * from utxo_asset_transfer_1542610800000_1542614399999 where PUBLICHASH='b813df4ed24854aa00864457452712ca473370f4294fe0349991047e09acb39e'"

	res, err = sql2kvRocksdb.Query(sql)

	if err != nil {
		panic(err)
	}
	for res.Next(&row) {
		fmt.Println("rowid:", row.RowId)
		for rowName, rowValue := range row.ColumnValue {
			name := rowName
			value := rowValue
			fmt.Println("name:", name, ",value:", value)
		}
	}

	fmt.Println("---------------------------------")
	sql = "select * from utxo_asset_transfer_1542610800000_1542614399999 where TXTYPE=4"

	res, err = sql2kvRocksdb.Query(sql)

	for res.Next(&row) {
		fmt.Println("rowid:", row.RowId)
		for rowName, rowValue := range row.ColumnValue {
			name := rowName
			value := rowValue
			fmt.Println("name:", name, ",value:", value)
		}
	}

	fmt.Println("---------------------------------")
	sql = "select * from utxo_asset_transfer_1542610800000_1542614399999 where HASH='05dfa58d91e64791e908d4ba8eeecbbb250ee493f0a4ffd1787e91b70226d96f'"

	res, err = sql2kvRocksdb.Query(sql)

	for res.Next(&row) {
		fmt.Println("rowid:", row.RowId)
		for rowName, rowValue := range row.ColumnValue {
			name := rowName
			value := rowValue
			fmt.Println("name:", name, ",value:", value)
		}
	}
	fmt.Println("---------------------------------")
}
