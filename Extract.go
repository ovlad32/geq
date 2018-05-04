package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/ovlad32/geq/dump"
	"github.com/pkg/errors"
)

var tableToExtract = flag.String("et", "", "")
var columnToExtract = flag.String("ec", "", "")
var extractCount = flag.Int("evc", 10, "")
var efcs = flag.Int("efcs", 1, "")
var efcp = flag.Int("efcp", 1, "")

func Extract() {
	var table *TableMap = nil

	if *tableToExtract == "" {
		panic("specify table name to extract data")
	}
	if *columnToExtract == "" {
		panic("specify column name to extract data")
	}

	conf, err := readConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}

	for _, tb := range conf.Tables {
		if strings.ToLower(tb.TableName) == strings.ToLower(*tableToExtract) {
			table = tb
			break
		}
	}
	if table == nil {
		panic(fmt.Sprintf("table %v not found in config file", *tableToExtract))
	}

	table.readHeader([]byte(conf.HeaderColumnSeparatorChar))

	colpos := -1
	for index, hb := range table.headers {
		if strings.ToLower(strings.TrimSpace(string(hb))) ==
			strings.ToLower(strings.TrimSpace(*columnToExtract)) {
			colpos = index
		}
	}
	if colpos == -1 {
		panic(fmt.Sprintf("column name %v not found in %v ", *columnToExtract, table.TableName))
	}

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
	var cache map[string]bool = make(map[string]bool)

	var proc4Extract dump.RowProcessingFuncType = func(
		cancelContext context.Context,
		config *dump.DumperConfigType,
		currentLineNumber uint64,
		currentStreamPosition uint64,
		cellsBytes [][]byte,
		rawLineBytes []byte,
	) (err error) {
		if len(cellsBytes) < colpos {
			return
		}
		cellBytes := cellsBytes[colpos]
		if len(cellBytes) == 0 {
			return
		}

		strippedCellBytes := cellBytes[1 : len(cellBytes)-1]
		var ref *[]byte = nil
		if *efcs == 1 {
			ref = &strippedCellBytes
		} else {
			fcellsBytes := bytes.Split(strippedCellBytes, []byte(conf.FusionSeparatorChar))
			if len(fcellsBytes) < *efcp {
				return
			}
			ref = &fcellsBytes[*efcp-1]
		}

		if ref == nil || len(*ref) == 0 {
			return
		}
		if (*extractCount) > 0 {
			sref := string(*ref)
			if _, found := cache[sref]; found {
				return
			} else {
				cache[sref] = true
			}
		}
		if table.writer == nil {
			if *pfout == "" {
				table.writer = os.Stdout
			} else {
				err = os.MkdirAll(*pfout, 0777)
				if err != nil {
					panic(err)
				}
				s := path.Join(*pfout, fmt.Sprintf("%v.%v.%v.%v",
					table.TableName,
					strings.TrimSpace(string(table.headers[colpos])),
					*efcp,
					*efcs,
				))
				table.writer, err = os.Create(s)
				if err != nil {
					panic(err)
				}
			}
		}
		_, err = table.writer.Write(*ref)

		if err != nil {
			panic(err)
		}
		_, err = table.writer.Write([]byte("\n"))
		if (*extractCount) > 0 && len(cache) == *extractCount {
			table.writer.Close()
			os.Exit(0)
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
	if table.writer != nil {
		table.writer.Close()
	}

}
