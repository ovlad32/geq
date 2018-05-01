package dump

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

//LineFeedByte \n   U+000A line feed or newline
const LineFeedByte = byte('\n')

//CarriageReturnByte \r   U+000D carriage return
const CarriageReturnByte = byte('\r')

func TruncateFromCRLF(line []byte) []byte {
	for _, value := range []byte{LineFeedByte, CarriageReturnByte} {
		if line[len(line)-1] == value {
			line = line[:len(line)-1]
		}
	}
	return line
}

func SplitDumpLine(line []byte, sep byte) [][]byte {
	return bytes.Split(TruncateFromCRLF(line), []byte{sep})
}

type DumperStartFromLine struct {
	Line uint64
}

type DumperStartFromByte struct {
	Position  int
	FirstLine uint64
}

type DumperConfigType struct {
	GZip                   bool
	ColumnSeparator        byte
	LineSeparator          byte
	BufferSize             int
	StartFromLine          *DumperStartFromLine
	StartFromByte          *DumperStartFromByte
	FusionColumnSeparators []byte
}

type DumperType struct {
	config DumperConfigType
}

func NewDumper(cfg *DumperConfigType) (dumper *DumperType, err error) {
	err = validateDumperConfig(cfg)
	if err != nil {
		err = fmt.Errorf("coult not create a new dumper: %v", err)
		return
	}
	return &DumperType{
		config: *cfg,
	}, nil
}

type errorAbortedByType struct {
	error
	message string
}

func (e errorAbortedByType) Error() string {
	return e.message
}

type ErrorAbortedByRowProcessing errorAbortedByType

type ErrorAbortedByContext errorAbortedByType

type RowProcessingFuncType func(
	cancelContext context.Context,
	config *DumperConfigType,
	currentLineNumber uint64,
	currentStreamPosition uint64,
	cellsBytes [][]byte,
	rawLineBytes []byte,
) (err error)

var (
	x0D                             = []byte{0x0D}
	defaultColumnSeparatorByte byte = 0x1F
	defaultLineSeparatorByte   byte = 0x0A
	defaultBufferSize          int  = 4096
)

func IsErrorAbortedByRowProcessing(err error) bool {
	if err == nil {
		return false
	}
	_, typeOf := err.(ErrorAbortedByRowProcessing)
	return typeOf
}

func IsErrorByContext(err error) bool {
	if err == nil {
		return false
	}
	_, typeOf := err.(ErrorAbortedByContext)
	return typeOf
}

func validateDumperConfig(cfg *DumperConfigType) (err error) {
	if cfg == nil {
		err = fmt.Errorf("DumperConfig is not initialized")
		return
	}

	if cfg.StartFromLine != nil && cfg.StartFromByte != nil &&
		cfg.StartFromLine.Line > 0 && cfg.StartFromByte.Position > 0 {
		err = fmt.Errorf(
			"wrong parameters: mixture of mutually exceptional parameters: "+
				"config.MoveToLine > %v && config.MoveToByte.Position>%v",
			cfg.StartFromLine.Line,
			cfg.StartFromByte.Position,
		)
		return

	}

	if cfg.ColumnSeparator == 0 {
		cfg.ColumnSeparator = defaultColumnSeparatorByte
	}

	if cfg.LineSeparator == 0 {
		cfg.LineSeparator = defaultLineSeparatorByte
	}

	if cfg.LineSeparator == 0 {
		cfg.BufferSize = defaultBufferSize
	}

	return
}
func (dumper *DumperType) Config() DumperConfigType {
	return dumper.config
}

func (dumper *DumperType) ReadFromStream(
	ctx context.Context,
	stream io.Reader,
	rowProcessingFunc RowProcessingFuncType,
) (lineNumber uint64, err error) {
	var streamPosition uint64

	if rowProcessingFunc == nil {
		err = fmt.Errorf(
			"row processing function must be defined",
		)
		return
	}

	err = validateDumperConfig(&dumper.config)
	if err != nil {
		err = fmt.Errorf("could not process dump stream: %v", err)
		return
	}

	if dumper.config.GZip {
		var zipped *gzip.Reader
		zipped, err = gzip.NewReader(stream)
		if err != nil {
			err = fmt.Errorf("couldn't create zip reader from stream: %v", err)
			return
		}
		stream = zipped
	}

	buffered := bufio.NewReaderSize(stream, dumper.config.BufferSize)
	if err != nil {
		err = fmt.Errorf("couldn't create buffer from stream: %v", err)
		return
	}

	if dumper.config.StartFromByte != nil && dumper.config.StartFromByte.Position > 0 {
		var discarded int
		discarded, err = buffered.Discard(dumper.config.StartFromByte.Position)
		if err != nil {
			err = fmt.Errorf("could not discard stream to position %v: %v",
				dumper.config.StartFromByte.Position,
				err,
			)
			return
		}

		if discarded != dumper.config.StartFromByte.Position {
			err = fmt.Errorf("discarded position mismatch %v, expected: %v",
				dumper.config.StartFromByte.Position,
				uint64(discarded),
			)
			return
		}
		lineNumber = dumper.config.StartFromByte.FirstLine
		streamPosition = uint64(discarded)
	}
	for {
		select {
		case <-ctx.Done():
			err = &ErrorAbortedByContext{}
			return
		default:
			var originalLine []byte
			originalLine, err = buffered.ReadSlice(dumper.config.LineSeparator)
			if err == io.EOF {
				return lineNumber, nil
			} else if err != nil {
				return lineNumber, fmt.Errorf(
					"couldn't read data from stream: %v",
					err,
				)
			}
			if dumper.config.StartFromLine == nil || lineNumber >= dumper.config.StartFromLine.Line {

				originalLineLength := len(originalLine)

				lineColumns := SplitDumpLine(originalLine, dumper.config.ColumnSeparator)

				err = rowProcessingFunc(ctx,
					&dumper.config,
					lineNumber,
					streamPosition,
					lineColumns,
					originalLine,
				)

				if err != nil {
					return
				}

				lineNumber++
				streamPosition += uint64(originalLineLength)
			}
		}
	}

}

func (dumper *DumperType) ReadFromFile(
	ctx context.Context,
	pathToFile string,
	rowProcessingFunc RowProcessingFuncType,
) (lineNumber uint64, err error) {

	if strings.TrimSpace(pathToFile) == "" {
		err = errors.New("pathToFile is empty")
		return
	}

	err = validateDumperConfig(&dumper.config)
	if err != nil {
		err = fmt.Errorf("could not process file %v : %v", pathToFile, err)
		return
	}

	file, err := os.Open(pathToFile)
	if err != nil {
		err = fmt.Errorf("could not open file %v: %v", pathToFile, err)
		return
	}

	defer file.Close()

	return dumper.ReadFromStream(ctx, file, rowProcessingFunc)

}
