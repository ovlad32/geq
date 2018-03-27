package main

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ovlad32/geq/dump"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type TableMap struct {
	TableName    string `json:"table_name"`
	PathToHeader string `json:"path_to_header"`
	PathToData   string `json:"path_to_data"`
	headers      [][]byte
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
	ResultColumnSeparatorByte int         `json:"header_column_separator_byte"`
}

var ptable = flag.String("t", "", "")
var pvalues = flag.String("v", "", "")
var psep = flag.String("s", "|", "")
var pfout = flag.String("o", "", "")

func main() {
	flag.Parse()
	if *ptable == "" || *pvalues == "" || *psep == "" {
		panic("Table, Value, Separator to find are needed!")
	}
	conf, err := ReadConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}

	dcs := byte(conf.DataColumnSeparatorByte)

	/*hcs, err := strconv.Atoi(conf.HeaderColumnSeparatorByte)
	if err != nil {
		err = errors.Wrapf(err, "could not convert to number %v", conf.HeaderColumnSeparatorByte)
		panic(err)
	}*/
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
	dir := func(p string) (result []string) {
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
					if ext == ".gz" {
						result = append(result, path.Join(curdir, f.Name()))
					}
				}
			}
		}
		return
	}

	pValuesBytes := bytes.Split([]byte(*pvalues), []byte(*psep))

	for _, t := range conf.Tables {
		if *ptable != t.TableName {
			continue
		}
		headerBytes, err := ioutil.ReadFile(t.PathToHeader)
		if err != nil {
			panic(err)
		}

		hb := bytes.Split(headerBytes, []byte(conf.HeaderColumnSeparatorChar))
		t.headers = make([][]byte, len(hb))
		for index := range hb {
			t.headers[index] = hb[index]
		}

		var proc dump.RowProcessingFuncType = func(
			cancelContext context.Context,
			config *dump.DumperConfigType,
			currentLineNumber uint64,
			currentStreamPosition uint64,
			cellBytes [][]byte,
			rawLineBytes []byte,
		) (err error) {
			allFound := true
			for _, val := range pValuesBytes {
				found := false
			cellBreak:
				for _, cell := range cellBytes {
					if len(cell) < 1 {
						continue
					}
					strippedCell := cell[1 : len(cell)-1]
					if bytes.Compare(strippedCell, val) == 0 {
						found = true
						break cellBreak
					}
					fcells := bytes.Split(cell, []byte{byte(dcs)})
					for _, fcel := range fcells {
						if bytes.Compare(fcel, val) == 0 {
							found = true
							break cellBreak
						}
					}
				}
				allFound = allFound && found
			}
			if allFound {
				if t.writer == nil {
					if *pfout == "" {
						t.writer = os.Stdout
					} else {
						t.writer, err = os.Create(*pfout)
					}
					if err != nil {
						panic(err)
					}

					_, err = t.writer.Write(
						bytes.Join([][]byte{[]byte("Line#"),
							bytes.Replace(
								headerBytes,
								[]byte(conf.HeaderColumnSeparatorChar),
								[]byte{byte(conf.ResultColumnSeparatorByte)},
								-1,
							)},
							[]byte{byte(conf.ResultColumnSeparatorByte)},
						))
					if err != nil {
						panic(err)
					}
					//_, err = t.writer.Write([]byte{dump.LineFeedByte})
					//if err != nil {
					//	panic(err)
					//}
				}
				_, err = t.writer.Write(
					bytes.Join([][]byte{
						[]byte(fmt.Sprintf("%v", currentLineNumber)),
						bytes.Replace(
							rawLineBytes,
							[]byte{byte(conf.DataColumnSeparatorByte)},
							[]byte{byte(conf.ResultColumnSeparatorByte)},
							-1,
						)},
						[]byte{byte(conf.ResultColumnSeparatorByte)},
					))

				if err != nil {
					panic(err)
				}
			}
			return
		}

		for _, filePath := range dir(t.PathToData) {
			log.Printf("%v...", filePath)
			_, err = dmp.ReadFromFile(
				context.Background(),
				filePath,
				proc,
			)

			if err != nil {
				err = errors.Wrapf(err, "could not read %v", filePath)
				panic(err)
			}
		}

	}
	for _, t := range conf.Tables {
		if t.writer != nil {
			t.writer.Close()
		}
	}
}

func ReadConfig() (result *TableMaps, err error) {
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
