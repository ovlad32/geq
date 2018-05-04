package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
	"github.com/ovlad32/geq/dump"
	"github.com/pkg/errors"
)

var conn = flag.String("conn", "user=postgres password=postgres dbname=postgres host=localhost port=5432 sslmode=disable", "")

var targetTable = flag.String("targetTable", "", "")
var sourceTable = flag.String("sourceTable", "", "")

func Pgu() {
	if *sourceTable == "" {
		panic("sourceTable is empty")
	}

	if *targetTable == "" {
		panic("targetTable is empty")
	}

	db, err := sql.Open("postgres", *conn)
	if err != nil {
		log.Fatal(err)
	}
	conf, err := readConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}
	var table *TableMap

	for _, tb := range conf.Tables {
		if strings.ToLower(tb.TableName) == strings.ToLower(*sourceTable) {
			table = tb
			break
		}
	}
	if table == nil {
		panic(fmt.Sprintf("table %v not found in config file", *sourceTable))
	}

	table.readHeader([]byte(conf.HeaderColumnSeparatorChar))
	dcs := byte(conf.DataColumnSeparatorByte)

	dc := &dump.DumperConfigType{
		ColumnSeparator: dcs,
		LineSeparator:   dump.LineFeedByte,
		GZip:            true,
		BufferSize:      4096,
	}

	dmp, err := dump.NewDumper(dc)
	if err != nil {
		err = errors.Wrapf(err, "could not create dumper")
		panic(err)
	}
	headers := make([]string, len(table.headers))
	for index := range table.headers {
		headers[index] = string(table.headers[index])
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	txCount := 0
	var proc4Extract dump.RowProcessingFuncType = func(
		cancelContext context.Context,
		config *dump.DumperConfigType,
		currentLineNumber uint64,
		currentStreamPosition uint64,
		cellsBytes [][]byte,
		rawLineBytes []byte,
	) (err error) {

		columns := make([]string, 0, len(headers))
		placeholders := make([]string, 0, len(headers))
		values := make([]interface{}, 0, len(headers))

		for index, cb := range cellsBytes {
			if len(cb) < 2 {
				continue
			}
			if index >= len(headers) {
				continue
			}
			cb = cb[1 : len(cb)-1]

			columns = append(columns, headers[index])
			placeholders = append(placeholders, fmt.Sprintf("$%v", index+1))
			values = append(values, string(cb))
		}
		if len(columns) > 0 {
			dml := fmt.Sprintf(
				"insert into %v(%v) values(%v)",
				*targetTable,
				strings.Join(columns, ","),
				strings.Join(placeholders, ","),
			)
			stmt, err := tx.Prepare(dml)
			if err != nil {
				log.Fatal(err.Error() + ": " + dml)
			}
			_, err = stmt.Exec(values...)
			if err != nil {
				log.Fatal(err)
			}
			txCount++
			if txCount >= 1000 {
				err = tx.Commit()
				if err != nil {
					log.Fatal(err)
				}
				txCount = 0
			}

		}
		return
	}

	for _, filePath := range allFiles(table.PathToData, ".gz") {

		log.Printf("%v...", filePath)
		_, err = dmp.ReadFromFile(
			context.Background(),
			filePath,
			proc4Extract,
		)

		if err != nil {
			err = errors.Wrapf(err, "could not read %v", filePath)
			panic(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	db.Close()

}
