// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const DefaultFalseRate = 0.001

var _ = RegistrySKFileReaderCreator(uint32(index.BloomFilterUniversal), &UniversalBFCreator{})

type UniversalBFCreator struct {
}

func (creator *UniversalBFCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewUniversalBloomFilterReader(rpnExpr, schema, option)
}

type UniversalBloomFilter struct {
	Filter *bloom.BloomFilter
}

type UniversalBloomFilterWriter struct {
	*skipIndexWriter
	falseRate      float64
	mu             sync.RWMutex
	serializedData map[string][]byte
	bloomFilters   []*UniversalBloomFilter
}

func (idx *UniversalBloomFilterWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	for _, id := range schemaIdx {
		field := writeRec.Schema[id]
		indexPath := path.Join(idx.dir, idx.msName, colstore.AppendSecondaryIndexSuffix(idx.dataFilePath, field.Name, index.BloomFilter, colstore.DefaultFileType)+tmpFileSuffix)
		if err := idx.BuildUniversalBloomFilter(writeRec, id, indexPath); err != nil {
			return err
		}
	}
	return nil
}

func (idx *UniversalBloomFilterWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}

func (idx *UniversalBloomFilterWriter) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for p, data := range idx.serializedData {
		idx.Open()
		writer, err := idx.GetWriter(p)
		if err != nil {
			return err
		}
		err = writer.WriteData(data)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewUniversalGeneralBloomFilterWriter(dir, msName, dataFilePath, lockPath, tokens string, params *influxql.IndexParam) *UniversalBloomFilterWriter {
	var falseRate float64
	if len(params.IList) == 0 {
		falseRate = DefaultFalseRate
	} else {
		if v, ok := params.IList[0].(*influxql.NumberLiteral); ok {
			falseRate = v.Val
		}
	}
	return &UniversalBloomFilterWriter{
		falseRate:       falseRate,
		serializedData:  make(map[string][]byte),
		bloomFilters:    make([]*UniversalBloomFilter, 0),
		skipIndexWriter: newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}
}

func (idx *UniversalBloomFilterWriter) NewUniversalBloomFilter() *UniversalBloomFilter {
	filter := bloom.NewWithEstimates(uint(util.RowsNumPerFragment), idx.falseRate)

	return &UniversalBloomFilter{
		Filter: filter,
	}
}

func (bf *UniversalBloomFilter) Test(value []byte) bool {
	return bf.Filter.Test(value)
}

func (bf *UniversalBloomFilter) Serialize() ([]byte, error) {
	data, err := bf.Filter.GobEncode()
	if err != nil {
		return nil, err
	}

	length := uint32(len(data))
	lengthBytes := make([]byte, util.Uint32SizeBytes)
	binary.LittleEndian.PutUint32(lengthBytes, length)

	result := append(lengthBytes, data...)
	return result, nil
}

func DeserializeBloomFilter(data []byte) (*UniversalBloomFilter, error) {
	filter := &bloom.BloomFilter{}
	if err := filter.GobDecode(data); err != nil {
		return nil, fmt.Errorf("failed to deserialize bloomFilter: %v", err)
	}

	return &UniversalBloomFilter{
		Filter: filter,
	}, nil
}

func (idx *UniversalBloomFilterWriter) BuildBloomFilterFromRecord(record *record.Record, colIdx int) (*UniversalBloomFilter, error) {
	if record == nil || colIdx < 0 || colIdx >= len(record.Schema) {
		return nil, fmt.Errorf("invalid params")
	}

	bf := idx.NewUniversalBloomFilter()
	if err := idx.addElementToBloomFilter(record, colIdx, bf); err != nil {
		return nil, err
	}

	return bf, nil
}

func (idx *UniversalBloomFilterWriter) addElementToBloomFilter(record *record.Record, colIdx int, bf *UniversalBloomFilter) error {
	colVal := &record.ColVals[colIdx]

	switch record.Schema[colIdx].Type {
	case influx.Field_Type_Int:
		addBytesToBfByFixLen(util.Int64SizeBytes, colVal.Val, bf)
	case influx.Field_Type_Float:
		addBytesToBfByFixLen(util.Float64SizeBytes, colVal.Val, bf)
	case influx.Field_Type_String:
		return addBytesToBfByArray(colVal.Offset, colVal.Val, bf)
	default:
		return fmt.Errorf("unsupported column type: %v", influx.FieldTypeName[record.Schema[colIdx].Type])
	}

	return nil
}

func addBytesToBfByArray(index []uint32, bytes []byte, bf *UniversalBloomFilter) error {
	pos := len(index)
	if pos == 0 {
		return errors.New("expect at least one string parameter")
	}
	for i := 0; i+1 < pos; i++ {
		bf.Filter.Add(bytes[index[i]:index[i+1]])
	}
	bf.Filter.Add(bytes[index[pos-1]:])
	return nil
}

func addBytesToBfByFixLen(fixLen int, bytes []byte, bf *UniversalBloomFilter) {
	for i := 0; i+fixLen <= len(bytes); i += fixLen {
		bf.Filter.Add(bytes[i : i+fixLen])
	}
}

func (idx *UniversalBloomFilterWriter) BuildUniversalBloomFilter(record *record.Record, colIdx int, indexDir string) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}

	bf, err := idx.BuildBloomFilterFromRecord(record, colIdx)
	if err != nil {
		return fmt.Errorf("failed to build bloomFilter %v", err)
	}

	data, err := bf.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize bloomFilter %v", err)
	}

	idx.mu.Lock()
	if existingData, exists := idx.serializedData[indexDir]; exists {
		idx.serializedData[indexDir] = append(existingData, data...)
	} else {
		idx.serializedData[indexDir] = data
	}
	idx.mu.Unlock()

	return nil
}

type UniversalBloomFilterReader struct {
	indexDir string
	mu       sync.RWMutex
	bfsMap   map[string][]*UniversalBloomFilter
	sk       SKCondition
	option   hybridqp.Options
}

func (r *UniversalBloomFilterReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r)
}

func (r *UniversalBloomFilterReader) GetFragmentRowCount(fragId uint32) (int64, error) {
	return 0, nil
}

func (r *UniversalBloomFilterReader) ReInit(file interface{}) error {
	// Initialize readers with file pathName information
	if f1, ok := file.(TsspFile); ok {
		filePath := f1.Path()
		if filePath == "" {
			return errors.New("file path cannot be empty")
		}

		dir, fileName := path.Split(filePath)

		fileName = strings.TrimSuffix(fileName, path.Ext(fileName))
		r.indexDir = path.Join(dir, fileName)
	} else {
		return errors.New("file need be tssp type")
	}

	return nil
}

func (r *UniversalBloomFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	switch elem.Op {
	case influxql.EQ:
		return isInBF(blockId, elem, r)
	default:
		return true, nil
	}
}

func isInBF(blockId int64, elem *rpn.SKRPNElement, r *UniversalBloomFilterReader) (bool, error) {
	r.indexDir = colstore.AppendSecondaryIndexSuffix(r.indexDir, elem.Key, index.BloomFilter, colstore.MinRowsForSeek)

	r.mu.RLock()
	if _, ok := r.bfsMap[r.indexDir]; !ok {
		r.mu.RUnlock()
		err := r.load()
		if err != nil {
			return false, err
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	fragId := uint32(blockId)
	if fragId >= uint32(len(r.bfsMap[r.indexDir])) {
		return false, errors.New("error fragId")
	}

	bf := r.bfsMap[r.indexDir][fragId]
	var bytes []byte
	switch elem.Value.(type) {
	case int64:
		bytes = make([]byte, util.Int64SizeBytes)
		binary.LittleEndian.PutUint64(bytes, uint64(elem.Value.(int64)))
	case float64:
		bytes = make([]byte, util.Float64SizeBytes)
		binary.LittleEndian.PutUint64(bytes, math.Float64bits(elem.Value.(float64)))
	case string:
		bytes = []byte(elem.Value.(string))
	default:
		return false, errors.New("error type of bf query")
	}
	return bf.Test(bytes), nil
}

func (r *UniversalBloomFilterReader) GetRowCount(blockId int64, elem *rpn.SKRPNElement) (int64, error) {
	return 0, nil
}

func (r *UniversalBloomFilterReader) StartSpan(span *tracing.Span) {

}

func NewUniversalBloomFilterReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options) (*UniversalBloomFilterReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}

	return &UniversalBloomFilterReader{
		sk:     sk,
		option: option,
		bfsMap: make(map[string][]*UniversalBloomFilter),
	}, nil
}

func (r *UniversalBloomFilterReader) Close() error {
	return nil
}

func (r *UniversalBloomFilterReader) load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, err := os.ReadFile(r.indexDir)
	if err != nil {
		return fmt.Errorf("cannot read index file: %v", err)
	}

	remainingData := data
	for len(remainingData) >= util.Uint32SizeBytes {
		length := binary.LittleEndian.Uint32(remainingData[0:util.Uint32SizeBytes])

		if len(remainingData) < int(length)+util.Uint32SizeBytes {
			break
		}

		bfData := remainingData[util.Uint32SizeBytes : uint32(util.Uint32SizeBytes)+length]

		bf, err := DeserializeBloomFilter(bfData)
		if err != nil {
			break
		}

		r.bfsMap[r.indexDir] = append(r.bfsMap[r.indexDir], bf)
		remainingData = remainingData[uint32(util.Uint32SizeBytes)+length:]
	}

	if len(r.bfsMap[r.indexDir]) == 0 {
		return errors.New("get 0 bf from file")
	}

	return nil
}
