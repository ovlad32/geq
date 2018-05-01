package main

import (
	"bytes"
	"container/list"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

var pfout = flag.String("o", "", "")
var pfin = flag.String("i", "", "")
var cmd = flag.String("c", "", "")

func main() {
	flag.Parse()

	if *pfout == "" {
		panic("Provide output directory name! -o=out")
	}

	if *cmd == "c" {
		JsonCheck()
	} else if *cmd == "e" {
		Extract()

	} else if *cmd == "f" {
		Filter()
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
