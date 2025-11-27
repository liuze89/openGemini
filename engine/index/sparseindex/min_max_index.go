// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparseindex

import (
	"errors"
	"fmt"
	"math"
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/spf13/cast"
)

const (
	MinValueColumnName  = "min_value"
	MaxValueColumnName  = "max_value"
	MinValueColumnIndex = 0
	MaxValueColumnIndex = 1
	EquivalentAccuracy  = 1e-7
)

var _ = RegistrySKFileReaderCreator(uint32(index.MinMax), &MinMaxReaderCreator{})

type MinMaxReaderCreator struct {
}

func (creator *MinMaxReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewMinMaxIndexReader(rpnExpr, schema, option, isCache)
}

// MinMaxIndexReader:
//  1. the data record. the fragment size is 3.
//     f1 f2
//     1  4
//     2  5
//     3  6
//  2. the index record
//     1  4  -> min value
//     3  6  -> max value

type MinMaxIndexReader struct {
	isCache bool
	option  hybridqp.Options
	schema  record.Schemas
	// read the data of the index according to the file and index fields.
	ReadFunc func(file interface{}, rec *record.Record, isCache bool) (*record.Record, error)
	sk       SKCondition
	readers  *MinMaxFilterReaders
	span     *tracing.Span
}

func NewMinMaxIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*MinMaxIndexReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	return &MinMaxIndexReader{
		schema:  schema,
		option:  option,
		isCache: isCache,
		sk:      sk,
	}, nil
}

func (r *MinMaxIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r.readers)
}

func (r *MinMaxIndexReader) GetFragmentRowCount(fragId uint32) (int64, error) {
	return 0, nil
}

func (r *MinMaxIndexReader) StartSpan(span *tracing.Span) {
	r.span = span
}

func (r *MinMaxIndexReader) Close() error {
	return nil
}

type MinMaxWriter struct {
	*skipIndexWriter
	mmIndexMap map[string]*record.Record
}

func NewMinMaxWriter(dir, msName, dataFilePath, lockPath string, tokens string) *MinMaxWriter {
	return &MinMaxWriter{
		skipIndexWriter: newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
		mmIndexMap:      make(map[string]*record.Record),
	}
}

func (m *MinMaxWriter) initMMIndex(field record.Field, path string) {
	if _, ok := m.mmIndexMap[path]; ok {
		return
	}
	m.mmIndexMap[path] = &record.Record{}
	minMaxSchema := record.Schemas{
		{
			Name: MinValueColumnName,
			Type: field.Type,
		},
		{
			Name: MaxValueColumnName,
			Type: field.Type,
		},
	}

	m.mmIndexMap[path].SetSchema(minMaxSchema)
	m.mmIndexMap[path].ReserveColVal(len(minMaxSchema))
}

func (m *MinMaxWriter) Flush() error {
	for filePath, rec := range m.mmIndexMap {
		if err := m.writeIndex(filePath, rec); err != nil {
			return err
		}
	}
	return nil
}

func (m *MinMaxWriter) writeIndex(filePath string, record *record.Record) error {
	indexBuilder := colstore.NewIndexBuilder(&m.lockPath, filePath)
	defer indexBuilder.Reset()
	return indexBuilder.WriteData(record, colstore.DefaultTCLocation)
}

func (m *MinMaxWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	for _, idx := range schemaIdx {
		field := writeRec.Schema[idx]
		indexFilePath := path.Join(m.dir, m.msName, colstore.AppendSecondaryIndexSuffix(m.dataFilePath, field.Name, index.MinMax, 0))
		m.initMMIndex(field, indexFilePath)
		for segIdx := 0; segIdx < len(rowsPerSegment); segIdx++ {
			startRow := 0
			if segIdx > 0 {
				startRow = rowsPerSegment[segIdx-1]
			}
			endRow := rowsPerSegment[segIdx]

			// Calculate minValue/maxValue for current segment
			minIndex, maxIndex := m.calculateSegmentMinMax(writeRec, idx, startRow, endRow)
			if minIndex == -1 || maxIndex == -1 {
				return errno.NewError(errno.UnsupportedDataType, "writeRec.Schema[idx].Type", writeRec.Schema[idx].Type)
			}

			m.mmIndexMap[indexFilePath].ColVals[MinValueColumnIndex].AppendByteSlice(writeRec.ColVals[idx].Val[util.Int64SizeBytes*minIndex : util.Int64SizeBytes*(minIndex+1)])
			m.mmIndexMap[indexFilePath].ColVals[MaxValueColumnIndex].AppendByteSlice(writeRec.ColVals[idx].Val[util.Int64SizeBytes*maxIndex : util.Int64SizeBytes*(maxIndex+1)])
		}

	}
	return nil
}

func (m *MinMaxWriter) calculateSegmentMinMax(writeRec *record.Record, fieldIdx, startRow, endRow int) (int, int) {
	colVal := writeRec.Column(fieldIdx)
	fieldType := writeRec.Schema[fieldIdx].Type

	switch fieldType {
	case influx.Field_Type_Int:
		values := colVal.IntegerValues()
		return calculateMinMaxIndices[int64](values, startRow, endRow, math.MaxInt64, math.MinInt64)
	case influx.Field_Type_Float:
		values := colVal.FloatValues()
		return calculateMinMaxIndices[float64](values, startRow, endRow, math.MaxFloat64, -math.MaxFloat64)
	default:
		return -1, -1
	}
}

func calculateMinMaxIndices[T Number](values []T, startRow, endRow int, initialMin, initialMax T) (int, int) {
	minIndex := 0
	maxIndex := 0
	minValue := initialMin
	maxValue := initialMax

	loopEnd := endRow
	if loopEnd > len(values) {
		loopEnd = len(values)
	}

	for i := startRow; i < loopEnd; i++ {
		if values[i] < minValue {
			minValue = values[i]
			minIndex = i
		}
		if values[i] > maxValue {
			maxIndex = i
			maxValue = values[i]
		}
	}

	return minIndex, maxIndex
}

func (m *MinMaxWriter) Close() error {
	for _, fw := range m.writers {
		fw.Reset()
	}
	return nil
}

type Number interface {
	~int64 | ~float64
}

// checkIsInMinMax checks if the min/max values match the condition
func checkIsInMinMax[T Number](elem *rpn.SKRPNElement, minValues, maxValues []T, blockId int64) (bool, error) {
	if len(minValues) != len(maxValues) || blockId > int64(len(minValues)-1) {
		return false, fmt.Errorf("the maximum and minimum values should appear in pairs,"+
			" and the blockId should be within the array range. len(minValues): %d, blockId: %v", len(minValues), blockId)
	}

	value := cast.ToFloat64(elem.Value)
	minValue := float64(minValues[blockId])
	maxValue := float64(maxValues[blockId])

	switch elem.Op {
	case influxql.EQ:
		return compareValues(value, minValue) >= 0 && compareValues(value, maxValue) <= 0, nil
	case influxql.NEQ:
		if compareValues(value, minValue) == 0 && compareValues(value, maxValue) == 0 {
			return false, nil
		}
	case influxql.LT:
		if compareValues(value, minValue) <= 0 {
			return false, nil
		}
	case influxql.LTE:
		return compareValues(value, minValue) >= 0, nil
	case influxql.GTE:
		return compareValues(value, maxValue) <= 0, nil
	case influxql.GT:
		if compareValues(value, maxValue) >= 0 {
			return false, nil
		}
	default:
	}
	return true, nil
}

// compareValues compares two values of the same type
func compareValues(a, b float64) int {
	if math.Abs(a-b) <= EquivalentAccuracy {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

// MinMaxFilterReaders manages multiple MinMaxFilterReaders for different fields
type MinMaxFilterReaders struct {
	path       string
	file       string
	ir         *influxql.IndexRelation
	schemas    record.Schemas
	indexCache map[string]*record.Record
	span       *tracing.Span
}

// NewMinMaxFilterReaders creates a new MinMaxFilterReaders
func NewMinMaxFilterReaders(path, file string, schemas record.Schemas, ir *influxql.IndexRelation) *MinMaxFilterReaders {
	return &MinMaxFilterReaders{
		path:       path,
		file:       file,
		ir:         ir,
		schemas:    schemas,
		indexCache: map[string]*record.Record{},
	}
}

// Close closes all readers
func (r *MinMaxFilterReaders) Close() error {
	return nil
}

// StartSpan starts a tracing span
func (r *MinMaxFilterReaders) StartSpan(span *tracing.Span) {
	r.span = span
}

// IsExist checks if the fragment might contain data matching the condition
func (r *MinMaxFilterReaders) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	rec, err := r.InitReaderAndQuery(elem)
	if err != nil {
		return false, err
	}
	switch rec.Schema.Field(0).Type {
	case influx.Field_Type_Int:
		minValues := rec.Column(MinValueColumnIndex).IntegerValues()
		maxValues := rec.Column(MaxValueColumnIndex).IntegerValues()
		return checkIsInMinMax(elem, minValues, maxValues, blockId)
	case influx.Field_Type_Float:
		minValues := rec.Column(MinValueColumnIndex).FloatValues()
		maxValues := rec.Column(MaxValueColumnIndex).FloatValues()
		return checkIsInMinMax(elem, minValues, maxValues, blockId)
	default:
		return true, nil
	}
}

// GetRowCount returns the row count for the fragment
func (r *MinMaxFilterReaders) GetRowCount(blockId int64, elem *rpn.SKRPNElement) (int64, error) {
	return 0, nil
}

// InitReaderAndQuery initializes a reader and gets the query string
func (r *MinMaxFilterReaders) InitReaderAndQuery(elem *rpn.SKRPNElement) (*record.Record, error) {
	if elem == nil {
		return nil, errors.New("the input SKRPNElement is nil")
	}

	// Find the field index
	idx := r.schemas.FieldIndex(elem.Key)
	if idx < 0 {
		return nil, fmt.Errorf("cannot find the index for the field: %s", elem.Key)
	}

	// Create new reader
	fileName := colstore.AppendSecondaryIndexSuffix(r.file, elem.Key, index.MinMax, colstore.MinRowsForSeek)
	fullName := path.Join(r.path, fileName)
	// The same skipReader, with identical index files, is read only once.
	if v, ok := r.indexCache[fullName]; ok {
		return v, nil
	}

	f, err := colstore.NewPrimaryKeyReader(fullName, &r.path)

	if err != nil {
		return nil, err
	}
	defer f.Close()

	rec, _, err := f.ReadData()

	if err != nil {
		return nil, err
	}

	// Put into cache
	r.indexCache[fullName] = rec
	return rec, nil
}

func (r *MinMaxIndexReader) ReInit(file interface{}) (err error) {
	// Initialize readers with file path information
	var path, fileName string
	if f1, ok := file.(TsspFile); ok {
		index := strings.LastIndex(f1.Path(), "/")
		if index == -1 {
			path = "/"
		} else {
			path = f1.Path()[:index]
		}
		fileName = strings.Split(f1.Path()[index+1:], ".")[0]
	} else {
		return errors.New("file need be tssp type")
	}
	r.readers = NewMinMaxFilterReaders(path, fileName, r.schema, nil)

	return nil
}

func (m *MinMaxWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}
