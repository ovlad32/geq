package main

import (
	"flag"
	"container/list"
	"strings"
	"io/ioutil"
	"github.com/pkg/errors"
	"path"
	"os"
	"path/filepath"
	"encoding/json"
	"io"
	"bytes"
	"fmt"
	"github.com/ovlad32/geq/dump"
	"log"
	"context"
)


var pfout = flag.String("o", "", "")
var pfin = flag.String("i", "", "")
var cmd = flag.String("c", "", "")
var et = flag.String("et", "", "")
var ec = flag.String("ec", "", "")
var efcs = flag.Int("efcs", 1, "")
var efcp = flag.Int("efcp", 1, "")


func main() {
	flag.Parse()


	if *pfout == "" {
		panic("Provide output directory name! -o=out")
	}

	if *cmd == "c" {
		JsonCheck()
	} else if *cmd == "e" {

	}
	conf, err := readConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}



	var table *TableMap = nil

	if *et == "" {
		panic("specify table name")
	}

	for _, tb := range conf.Tables {
		if strings.ToLower(tb.TableName) == strings.ToLower(*et) {
			table = tb
			break
		}
	}
	if table == nil {
		panic(fmt.Sprintf("table %v not found in config file",*et))
	}

	table.readHeader([]byte(conf.HeaderColumnSeparatorChar))

	if *ec == "" {
		panic("specify column name")
	}
	colpos := -1;
	for index ,hb := range table.headers {
		if strings.ToLower(strings.TrimSpace(string(hb))) ==
			strings.ToLower(strings.TrimSpace(*ec)) {
				colpos  = index
		}
	}
	if colpos == -1 {
		panic(fmt.Sprintf("column name %v not found in %v ",*ec,table.TableName))
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

		strippedCellBytes := cellBytes[1 : len(cellBytes)-1]
		var ref *[]byte = nil
		if *efcs == 1 {
			ref = &strippedCellBytes
		} else {
			fcellsBytes := bytes.Split(strippedCellBytes, []byte(conf.FusionSeparatorChar))
			if len(fcellsBytes) < *efcp {
				return
			}
			ref = &fcellsBytes[*efcp - 1]
		}
		if ref == nil || len(*ref) == 0 {
			return
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
				table.writer,err = os.Create(s)
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




func split(path string) (dir, file string) {
	i := strings.LastIndex(path, "\\")
	if i == -1 {
		i = strings.LastIndex(path, "/")
	}
	return path[:i+1], path[i+1:]
}

func allFiles(p, pext string) (result []string) {
	result = make([]string, 0, 10)
	all := list.New()
	all.PushBack(p)
	for {
		if all.Len() == 0 {
			break
		}
		elem := all.Front()
		curdir := elem.Value.(string)

		all.Remove(elem)
		fi, err := ioutil.ReadDir(curdir)
		if err != nil {
			err = errors.Wrapf(err, "could not read directory contents %v", curdir)
			panic(err)
		}
		for _, f := range fi {
			if f.IsDir() {
				all.PushBack(path.Join(curdir, f.Name()))
			} else {
				ext := path.Ext(strings.ToLower(f.Name()))
				if ext == pext {
					result = append(result, path.Join(curdir, f.Name()))
				}
			}
		}
	}
	return
}




type TableMap struct {
	TableName      string `json:"table_name"`
	PathToHeader   string `json:"path_to_header"`
	PathToData     string `json:"path_to_data"`
	allHeaderBytes []byte
	headers        [][]byte
	//headerFlags[]bool
	allFiles []string
	//fusions map[int]map[int]int //Map[colPosition]map[FusSize]FusPos
	file   *os.File
	writer io.WriteCloser
}

type TableMaps struct {
	Tables                    []*TableMap `json:"tables"`
	DataColumnSeparatorByte   int         `json:"data_column_separator_byte"`
	FusionSeparatorChar       string      `json:"fusion_separator_char"`
	HeaderColumnSeparatorChar string      `json:"header_column_separator_char"`
	ResultColumnSeparatorByte int         `json:"result_column_separator_byte"`
	FusionColumnSizeAlignment int         `json:"fusion_column_size_alignment"`
}



func readConfig() (result *TableMaps, err error) {
	ex, err := os.Executable()
	exPath := filepath.Dir(ex)

	pathToConfigFile := filepath.Join(exPath, "config.json")

	if _, err := os.Stat(pathToConfigFile); os.IsNotExist(err) {
		err = errors.Wrapf(err, "Specify correct path to config.json")
		return nil, err
	}

	conf, err := os.Open(pathToConfigFile)
	if err != nil {
		err = errors.Wrapf(err, "Opening config file %v", pathToConfigFile)
		return nil, err
	}
	jd := json.NewDecoder(conf)
	result = new(TableMaps)
	err = jd.Decode(result)
	if err != nil {
		err = errors.Wrapf(err, "Decoding config file %v", pathToConfigFile)
		return nil, err
	}

	return result, nil
}

func (t *TableMap) readHeader(sep []byte) {
	var err error
	t.allHeaderBytes, err = ioutil.ReadFile(t.PathToHeader)
	if err != nil {
		panic(err)
	}

	hb := bytes.Split(t.allHeaderBytes, sep)
	t.headers = make([][]byte, len(hb))
	for index := range hb {
		t.headers[index] = hb[index]
	}
}