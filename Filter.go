package main

import (
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

var tableToFilter = flag.String("ft", "", "")
var columnToFilter = flag.String("fc", "", "")
var valueToFilter = flag.String("fv", "", "")
var valueToEmptyEmpty = flag.Bool("fvempty", false, "")
var filterOperator = flag.String("fo", "e", "")

func Filter() {

	var table *TableMap = nil

	if *tableToFilter == "" {
		panic("specify table name to filter data")
	}
	if *columnToFilter == "" {
		panic("specify column name to filter data")
	}
	if *filterOperator == "" {
		panic("specify filter operator: e=equal,p=prefix,s=suffix,i=inclusion")
	}
	if !*valueToEmptyEmpty && *columnToFilter == "" {
		panic("specify value to filter data")
	}

	conf, err := readConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}

	for _, tb := range conf.Tables {
		if strings.ToLower(tb.TableName) == strings.ToLower(*tableToFilter) {
			table = tb
			break
		}
	}
	if table == nil {
		panic(fmt.Sprintf("table %v not found in config file", *tableToFilter))
	}

	table.readHeader([]byte(conf.HeaderColumnSeparatorChar))

	colpos := -1
	for index, hb := range table.headers {
		if strings.ToLower(strings.TrimSpace(string(hb))) ==
			strings.ToLower(strings.TrimSpace(*columnToFilter)) {
			colpos = index
		}
	}
	if colpos == -1 {
		panic(fmt.Sprintf("column name %v not found in %v ", *columnToFilter, table.TableName))
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

	var proc4Filter dump.RowProcessingFuncType = func(
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
		var ref *[]byte = &strippedCellBytes
		/*if *efcs == 1 {
			ref = &strippedCellBytes
		} else {
			fcellsBytes := bytes.Split(strippedCellBytes, []byte(conf.FusionSeparatorChar))
			if len(fcellsBytes) < *efcp {
				return
			}
			ref = &fcellsBytes[*efcp-1]
		}*/

		if (ref == nil || len(*ref) == 0) && !*valueToEmptyEmpty {
			return
		}
		sref := string(*ref)
		switch *filterOperator {
		case "e":
			if sref != *valueToFilter {
				return
			}
		case "p":
			if !strings.HasPrefix(sref, *valueToFilter) {
				return
			}
		case "s":
			if !strings.HasSuffix(sref, *valueToFilter) {
				return
			}
		case "i":
			if !strings.Contains(sref, *valueToFilter) {
				return
			}
		default:
			panic(fmt.Sprintf("filter operator value, -fo option, (%v) is not recognized", *filterOperator))
		}

		if table.writer == nil {
			if *pfout == "" {
				table.writer = os.Stdout
			} else {
				err = os.MkdirAll(*pfout, 0777)
				if err != nil {
					panic(err)
				}
				s := path.Join(*pfout, fmt.Sprintf("%v.%v",
					table.TableName,
					*valueToFilter,
				))
				table.writer, err = os.Create(s)
				if err != nil {
					panic(err)
				}
			}
		}
		_, err = table.writer.Write(rawLineBytes)

		if err != nil {
			panic(err)
		}
		//_, err = table.writer.Write([]byte("\n"))
		return
	}

	for _, filePath := range allFiles(table.PathToData, ".gz") {

		log.Printf("%v...", filePath)
		_, err = dmp.ReadFromFile(
			context.Background(),
			filePath,
			proc4Filter,
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
