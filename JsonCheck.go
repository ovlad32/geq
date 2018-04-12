package main

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/ovlad32/geq/dump"
	"strings"
	"fmt"
	"log"
	"strconv"
	"os"
	"path"
	"encoding/json"
	"sync"
	"context"
	"flag"
)


var pleft = flag.String("l", "", "")
var pright = flag.String("r", "", "")


func JsonCheck() {
	if *pfin == "" {
		panic("Provide json directory name! -i=pathToJsonFileDirectory")
	}
	matchedRows, err := readJoinFiles(*pfin)
	if err != nil {
		err = errors.Wrapf(err, "could not read input")
		panic(err)
	}
	if len(matchedRows) == 0 {
		err = errors.New("Match result set is empty!")
	}

	conf, err := readConfig()
	if err != nil {
		err = errors.Wrapf(err, "could not read config")
		panic(err)
	}



	var leftTable, rightTable *TableMap

	for _, tb := range conf.Tables {
		if leftTable == nil &&
			strings.ToLower(tb.TableName) == strings.ToLower(*pleft) {
			leftTable = tb
		}
		if rightTable == nil &&
			strings.ToLower(tb.TableName) == strings.ToLower(*pright) {
			rightTable = tb
		}
		if rightTable != nil && leftTable != nil {
			break
		}
	}
	if leftTable == nil && *pleft != "" {
		panic(
			fmt.Sprintf("Given Left table name %v not found in config", *pleft),
		)
	}
	if rightTable == nil && *pright != "" {
		panic(
			fmt.Sprintf("Given Right table name %v not found in config", *pright),
		)
	}
	if rightTable == leftTable {
		if rightTable == nil {
			panic("Provide left,right or both table name(s)! -l=your_left_table -r=your_right_table")
		}
		//cloning a table info to avoid its writer mutual usage
		tmp := *leftTable
		leftTable = &tmp
	}

	if len(matchedRows[0].Joins) == 0 {
		err = errors.New("Match result Join is empty")
	}

	if len(matchedRows[0].Joins[0].RightColumns) == 0 {
		err = errors.New("Match result rightColumns is empty")
	}

	log.Printf("join result left table is %v, right table is %v...",
		matchedRows[0].Joins[0].LeftTable,
		matchedRows[0].Joins[0].RightColumns[0].RightTable,
	)

	type tcolval struct {
		ref          *MatchedJoin
		colpos       int
		fcolpos      int
		fcolsize     int
		val          []byte
		found        bool
		jsonFileName string
		matchedRow   string
	}

	//pValuesBytes := bytes.Split([]byte(*pvalues), []byte(*psep))
	if leftTable != nil {
		leftTable.readHeader([]byte(conf.HeaderColumnSeparatorChar))
	}
	if rightTable != nil {
		rightTable.readHeader([]byte(conf.HeaderColumnSeparatorChar))
	}

	leftRows := make([][]*tcolval, 0, len(matchedRows))
	rightRows := make([][]*tcolval, 0, len(matchedRows))
	for _, row := range matchedRows {
		leftColumns := make([]*tcolval, 0, 400)
		rightColumns := make([]*tcolval, 0, 400)
		rightMap := make(map[string]*tcolval)
		for _, jl := range row.Joins {
			tcl := &tcolval{
				val:          []byte(jl.Value),
				matchedRow:   strconv.Itoa(row.MatchedRow),
				jsonFileName: row.fileName,
			}
			if jl.LeftSize <= 0 {
				fmt.Sprintf("Left Fusion Column Size is '%v'<=0 at %v.%v",
					jl.LeftSize, jl.LeftTable, jl.LeftColumn)
			}
			if jl.LeftPosition <= 0 {
				fmt.Sprintf("Left Fusion Column Position is '%v'<=0 at %v.%v",
					jl.LeftPosition, jl.LeftTable, jl.LeftColumn)
			}
			if leftTable != nil {
				cfound := false
				for pos0, hb := range leftTable.headers {
					if jl.LeftColumn == strings.TrimSpace(string(hb)) {
						tcl.ref = jl
						tcl.colpos = pos0
						tcl.fcolsize = jl.LeftSize
						tcl.fcolpos = jl.LeftPosition
						cfound = true
					}
				}
				if !cfound {
					panic(fmt.Sprintf("left column %v.%v is not found at %v",
						jl.LeftTable, jl.LeftColumn, leftTable.TableName,
					))
				}
				leftColumns = append(leftColumns, tcl)
			}
			if rightTable != nil {
				for _, jr := range jl.RightColumns {
					rname := fmt.Sprintf(
						"%v/%v/%v",
						jr.RightColumn,
						jr.RightPosition,
						jr.RightSize)
					if jr.RightSize <= 0 {
						fmt.Sprintf("Right Fusion Column Size is '%v'<=0 at %v.%v",
							jr.RightSize, jr.RightTable, jr.RightColumn)
					}
					if jr.RightPosition <= 0 {
						fmt.Sprintf("Right Fusion Column Position is '%v'<=0 at %v.%v",
							jr.RightPosition, jr.RightTable, jr.RightColumn)
					}

					if _, rfound := rightMap[rname]; !rfound {
						tcr := &tcolval{
							val:          []byte(jl.Value),
							matchedRow:   strconv.Itoa(row.MatchedRow),
							jsonFileName: row.fileName,
						}
						cfound := false

						for pos0, hb := range rightTable.headers {
							// fmt.Println(string(hb))
							if jr.RightColumn == strings.TrimSpace(string(hb)) {
								tcr.ref = jl
								tcr.colpos = pos0
								tcr.fcolsize = jr.RightSize
								tcr.fcolpos = jr.RightPosition
								cfound = true
							}
						}
						if !cfound {
							panic(fmt.Sprintf("right column %v.%v is not found at %v",
								jr.RightTable, jr.RightColumn, rightTable.TableName,
							))
						}
						rightMap[rname] = tcr
					}
				}
			}
			for _, tcr := range rightMap {
				rightColumns = append(rightColumns, tcr)
			}
		}
		leftRows = append(leftRows, leftColumns)
		rightRows = append(rightRows, rightColumns)
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

	check := func(t *TableMap, rows [][]*tcolval, fileSuffix string) {
		dumpFile := ""
		var proc4Check dump.RowProcessingFuncType = func(
			cancelContext context.Context,
			config *dump.DumperConfigType,
			currentLineNumber uint64,
			currentStreamPosition uint64,
			cellsBytes [][]byte,
			rawLineBytes []byte,
		) (err error) {
			for _, cols := range rows {
				var found bool
				for _, jc := range cols {
					found = false
					if len(cellsBytes) <= jc.colpos {
						panic(
							fmt.Sprintf(
								"# of columns in %v is less than current column zeroed-position %v",
								t.TableName, jc.colpos,
							),
						)
					}
					cellBytes := cellsBytes[jc.colpos]
					if len(cellBytes)-2 >= len(jc.val) {
						strippedCellBytes := cellBytes[1 : len(cellBytes)-1]
						if jc.fcolsize == 1 {
							if bytes.Compare(strippedCellBytes, jc.val) == 0 {
								found = true
							}
						} else {
							fcellsBytes := bytes.Split(strippedCellBytes, []byte(conf.FusionSeparatorChar))
							if len(fcellsBytes) == jc.fcolsize+conf.FusionColumnSizeAlignment {
								if bytes.Compare(fcellsBytes[jc.fcolpos-1], jc.val) == 0 {
									found = true
								}
							}
						}
					}
					if !found {
						break
					}
				}
				if found {
					for _, v := range cols {
						v.found = true
					}

					if t.writer == nil {
						if *pfout == "" {
							t.writer = os.Stdout
						} else {
							err = os.MkdirAll(*pfout, 0777)
							if err != nil {
								panic(err)
							}
							s := path.Join(*pfout, t.TableName+fileSuffix)
							t.writer, err = os.Create(s)
						}
						if err != nil {
							panic(err)
						}

						_, err = t.writer.Write(
							bytes.Join([][]byte{
								[]byte("\"IOTahoe_file_name\""),
								[]byte("\"IOTahoe_file_line\""),
								[]byte("\"GE_source_file_name\""),
								[]byte("\"GE_source_file_line\""),
								bytes.Replace(
									t.allHeaderBytes,
									[]byte(conf.HeaderColumnSeparatorChar),
									[]byte{byte(conf.ResultColumnSeparatorByte)},
									-1,
								)},
								[]byte{byte(conf.ResultColumnSeparatorByte)},
							))
						if err != nil {
							panic(err)
						}
					}
					line := bytes.Join([][]byte{
						[]byte("\"" + cols[0].jsonFileName + "\""),
						[]byte("\"" + cols[0].matchedRow + "\""),
						[]byte("\"" + dumpFile + "\""),
						[]byte(fmt.Sprintf("\"%v\"", currentLineNumber)),
						bytes.Replace(
							rawLineBytes,
							[]byte{byte(conf.DataColumnSeparatorByte)},
							[]byte{byte(conf.ResultColumnSeparatorByte)},
							-1,
						)},
						[]byte{byte(conf.ResultColumnSeparatorByte)},
					)

					_, err = t.writer.Write(line)

					if err != nil {
						panic(err)
					}
				}
			}
			return
		}

		for _, filePath := range allFiles(t.PathToData, ".gz") {
			log.Printf("%v...", filePath)
			_, dumpFile = split(filePath)
			_, err = dmp.ReadFromFile(
				context.Background(),
				filePath,
				proc4Check,
			)

			if err != nil {
				err = errors.Wrapf(err, "could not read %v", filePath)
				panic(err)
			}
		}
		if t.writer != nil {
			t.writer.Close()
		}

	}

	printReport := func(rows [][]*tcolval, side string) {
		header := false
		for _, cols := range rows {
			if !cols[0].found {
				var b bytes.Buffer
				enc := json.NewEncoder(&b)
				err = enc.Encode(cols[0].ref)
				if err != nil {
					panic(err)
				}
				if !header {
					log.Println(side + " Entry(-ies) not found:")
					header = true
				}
				log.Println(b.String())
				log.Println()
			}
		}
	}
	var wg sync.WaitGroup

	_, inFile := split(*pfin)
	if leftTable != nil {
		wg.Add(1)
		go func() {
			check(leftTable, leftRows, ".left."+inFile+".tsv")
			wg.Done()
		}()
	}
	if rightTable != nil {
		wg.Add(1)
		go func() {
			check(rightTable, rightRows, ".right."+inFile+".tsv")
			wg.Done()
		}()
	}
	wg.Wait()
	if leftTable != nil {
		printReport(leftRows, "Left")
	}
	if rightTable != nil {
		printReport(rightRows, "Right")
	}

}


func readJoinFiles(pfin string) (result []*MatchResult, err error) {
	s, err := os.Stat(pfin)
	if err != nil {
		panic(err)
	}
	var files = make([]string, 0, 0)
	if s.IsDir() {
		files = allFiles(pfin, ".json")
	} else {
		ext := path.Ext(strings.ToLower(pfin))
		if ext == ".json" {
			files = make([]string, 0, 0)
			files = append(files, pfin)
		}
	}
	if len(files) == 0 {
		panic("resultant json files not found")
	}
	fmt.Printf("Files to process:\n")

	result = make([]*MatchResult, 0, len(files)*1000)
	for _, pathToJsonFile := range files {
		_, fileName := split(pathToJsonFile)
		conf, err := os.Open(pathToJsonFile)
		if err != nil {
			err = errors.Wrapf(err, "Opening input file %v", pathToJsonFile)
			return nil, err
		}

		jd := json.NewDecoder(conf)
		rows := make([]*MatchResult, 0, 1)
		err = jd.Decode(&rows)
		if err != nil {
			err = errors.Wrapf(err, "Decoding input file %v", pathToJsonFile)
			return nil, err
		}
		for _, r := range rows {
			r.fileName = fileName
		}
		conf.Close()
		result = append(result, rows...)
		fmt.Printf("%v\n", pathToJsonFile)
	}
	return result, nil
}

type MatchResultSlice *[]MatchResult
type MatchResult struct {
	MatchedRow int `json:"matchedRow"`
	fileName   string
	Joins      []*MatchedJoin
}
type MatchedJoin struct {
	Value        string                `json:"value"`
	LeftTable    string                `json:"leftTable"`
	LeftColumn   string                `json:"leftColumn"`
	LeftPosition int                   `json:"leftColumnFusionPosition"`
	LeftSize     int                   `json:"leftColumnFusionSize"`
	RightColumns []*MatchedRightColumn `json:"rightColumns"`
}
type MatchedRightColumn struct {
	RightTable    string `json:"rightTable"`
	RightColumn   string `json:"rightColumn"`
	RightPosition int    `json:"rightColumnFusionPosition"`
	RightSize     int    `json:"rightColumnFusionSize"`
}


