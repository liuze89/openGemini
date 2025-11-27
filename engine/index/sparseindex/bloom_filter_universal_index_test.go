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

package sparseindex_test

import (
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniversalBloomFilterReader(t *testing.T) {
	reader := getBFReader(t)

	// Test StartSpan
	reader.StartSpan(nil)

	// Test Close
	assert.NoError(t, reader.Close())
}

func getBFReader(t *testing.T) *sparseindex.UniversalBloomFilterReader {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewUniversalBloomFilterReader(rpnExpr, schema, option)
	if err != nil {
		t.Fatal(err)
	}

	// Test GetFragmentRowCount
	rowCount, err := reader.GetFragmentRowCount(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowCount)
	return reader
}

func TestUniversalBloomFilterReaderInteger(t *testing.T) {
	// [1, 2, 3], [4, 5, 6] frag=1
	testCaseBF(t, influxql.EQ, influxql.Integer, "field1", int64(2), true)
	testCaseBF(t, influxql.EQ, influxql.Integer, "field1", int64(100), false)
}

func testCaseBF(t *testing.T, operator influxql.Token, numType influxql.DataType, fieldName string, condValue interface{}, expectResult bool) {
	reader, err := buildReaderBF(t, operator, numType, fieldName, condValue)
	require.NoError(t, err)
	defer reader.Close()

	pathName := t.TempDir()
	err = os.MkdirAll(path.Join(pathName, "mst"), 0700)
	require.NoError(t, err)
	files, err := WriteDataBF(t, pathName)
	assert.NoError(t, err)

	for _, name := range files {
		require.True(t, len(name) >= 5)
		err := fileops.RenameFile(name, name[:len(name)-5])
		require.NoError(t, err)
	}

	dataFile := "mst.tssp"
	tsspFile := &MockTsspFileBF{
		path: path.Join(pathName, "mst", dataFile),
		name: dataFile,
	}
	err = reader.ReInit(tsspFile)
	require.NoError(t, err)
	exist, err := reader.MayBeInFragment(0)
	assert.NoError(t, err)
	assert.Equal(t, expectResult, exist)
}

func TestUniversalBloomFilterReaderFloat(t *testing.T) {
	// [1.1, 2.2, 3.3], [4.4, 5.5, 6.6] fragId=1
	testCaseBF(t, influxql.EQ, influxql.Float, "field2", float64(2.2), true)
	testCaseBF(t, influxql.EQ, influxql.Float, "field2", float64(0.0), false)
}

func TestUniversalBloomFilterReaderString(t *testing.T) {
	// ["hello", "world", "test"], ["foo", "bar", "baz"] fragId=1
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "hello", true)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "world", true)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "test", true)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "foo", true)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "baz", true)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "unknown", false)
	testCaseBF(t, influxql.EQ, influxql.String, "field3", "", false)
}

func WriteDataBF(t *testing.T, pathName string) ([]string, error) {
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "mst", "mst", path.Join(pathName, "lockPath"), "tokens", &influxql.IndexParam{})
	// Create test record with multiple fields
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Float},
		{Name: "field3", Type: influx.Field_Type_String},
	}, false)

	// Add test data
	writeRec.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)
	writeRec.ColVals[1].AppendFloats(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)
	writeRec.ColVals[2].AppendStrings("hello", "world", "test", "foo", "bar", "baz")

	// Test with single field
	schemaIdx := []int{0, 1, 2}
	rowsPerSegment := []int{3, 6}
	err := writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)
	files := writer.Files()

	return files, writer.Close()
}

func buildReaderBF(test *testing.T, token influxql.Token, dataType influxql.DataType, fieldName string, condValue interface{}) (*sparseindex.UniversalBloomFilterReader, error) {
	var t int
	var option *query.ProcessorOptions
	if dataType == influxql.Integer {
		t = influx.Field_Type_Int
		if v, ok := condValue.(int64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.IntegerLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	} else if dataType == influxql.Float {
		t = influx.Field_Type_Float
		if v, ok := condValue.(float64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.NumberLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	} else {
		t = influx.Field_Type_String
		if v, ok := condValue.(string); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.StringLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	}
	schema := record.Schemas{{Name: fieldName, Type: t}}

	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	return sparseindex.NewUniversalBloomFilterReader(rpnExpr, schema, option)
}

type MockTsspFileBF struct {
	sparseindex.TsspFile
	path string
	name string
}

func (m MockTsspFileBF) Path() string {
	return m.path
}

func (m MockTsspFileBF) Name() string {
	return ""
}

func TestUniversalBloomFilterWriter(t *testing.T) {
	// Test NewUniversalGeneralBloomFilterWriter
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(t.TempDir(), "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	assert.NotNil(t, writer)

	// Test that Close doesn't panic
	assert.NotPanics(t, func() {
		err := writer.Close()
		assert.NoError(t, err)
	})
}

func TestUniversalBloomFilterWriter_CreateAttachIndex(t *testing.T) {
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter("/test", "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})

	// Create test record with multiple fields
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Float},
		{Name: "field3", Type: influx.Field_Type_String},
	}, false)

	// Add test data
	writeRec.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)
	writeRec.ColVals[1].AppendFloats(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)
	writeRec.ColVals[2].AppendStrings("hello", "world", "test", "foo", "bar", "baz")

	// Test with single field
	schemaIdx := []int{0}
	rowsPerSegment := []int{3, 6}
	err := writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test with multiple fields
	schemaIdx = []int{0, 1, 2}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test with empty rowsPerSegment
	rowsPerSegment = []int{0}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestUniversalBloomFilterWriter_CreateDetachIndex(t *testing.T) {
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter("/test", "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})

	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)

	dataBufs := make([][]byte, 0)

	_, strings := writer.CreateDetachIndex(writeRec, []int{0}, []int{3}, dataBufs)
	assert.True(t, strings == nil)

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestUniversalBloomFilterWriter_Flush(t *testing.T) {
	pathName := t.TempDir()
	err := os.MkdirAll(path.Join(pathName, "mst"), 0700)
	require.NoError(t, err)

	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "mst", "mst", path.Join(pathName, "lockPath"), "tokens", &influxql.IndexParam{})

	// Create test record
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)
	writeRec.ColVals[0].AppendIntegers(1, 2, 3)

	// Create index
	schemaIdx := []int{0}
	rowsPerSegment := []int{3}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test Flush
	err = writer.Flush()
	assert.NoError(t, err)

	// Test Rename
	files := writer.Files()
	err = writer.Close()
	require.NoError(t, err)
	for _, name := range files {
		require.True(t, len(name) >= 5)
		err := fileops.RenameFile(name, name[:len(name)-5])
		require.NoError(t, err)
	}

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestUniversalBloomFilter_NewUniversalBloomFilter(t *testing.T) {
	pathName := t.TempDir()
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	// Test NewUniversalBloomFilter
	bf := writer.NewUniversalBloomFilter()
	assert.NotNil(t, bf)
}

func TestUniversalBloomFilter_AddAndTestString(t *testing.T) {
	pathName := t.TempDir()
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	bf := writer.NewUniversalBloomFilter()

	// Test adding values
	bf.Filter.AddString("hello")
	bf.Filter.AddString("world")
	bf.Filter.AddString("test")

	// Test existing values
	assert.True(t, bf.Filter.TestString("hello"))
	assert.True(t, bf.Filter.TestString("world"))
	assert.True(t, bf.Filter.TestString("test"))

	// Test non-existing values (may have false positives)
	assert.False(t, bf.Filter.TestString("foo"))
	assert.False(t, bf.Filter.TestString("bar"))
	assert.False(t, bf.Filter.TestString("unknown"))
}

func TestUniversalBloomFilter_SerializeAndDeserialize(t *testing.T) {
	pathName := t.TempDir()
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	// Create and populate bloom Filter
	bf := writer.NewUniversalBloomFilter()
	bf.Filter.AddString("hello")
	bf.Filter.AddString("world")

	// Test serialization
	serialized, e := bf.Serialize()
	assert.NoError(t, e)
	assert.NotNil(t, serialized)
	assert.Greater(t, len(serialized), 0)

	length := binary.LittleEndian.Uint32(serialized[0:util.Uint32SizeBytes])

	require.True(t, len(serialized) >= int(length)+util.Uint32SizeBytes)

	bfData := serialized[util.Uint32SizeBytes : uint32(util.Uint32SizeBytes)+length]

	// Test deserialization
	deserialized, err := sparseindex.DeserializeBloomFilter(bfData)
	require.NoError(t, err)
	assert.NotNil(t, deserialized)

	// Test that deserialized Filter works correctly
	assert.True(t, deserialized.Filter.TestString("hello"))
	assert.True(t, deserialized.Filter.TestString("world"))
	assert.False(t, deserialized.Filter.TestString("unknown"))
}

func TestUniversalBloomFilterWriter_BuildBloomFilterFromRecord(t *testing.T) {
	pathName := t.TempDir()
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	// Create test record
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Float},
		{Name: "field3", Type: influx.Field_Type_String},
	}, false)
	writeRec.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)
	writeRec.ColVals[1].AppendFloats(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)
	writeRec.ColVals[2].AppendStrings("hello", "world", "test", "foo", "bar", "baz")

	// Test BuildBloomFilterFromRecord for integer field
	bf, err := writer.BuildBloomFilterFromRecord(writeRec, 0)
	require.NoError(t, err)
	assert.NotNil(t, bf)

	// Test BuildBloomFilterFromRecord for float field
	bf, err = writer.BuildBloomFilterFromRecord(writeRec, 1)
	require.NoError(t, err)
	assert.NotNil(t, bf)

	// Test BuildBloomFilterFromRecord for string field
	bf, err = writer.BuildBloomFilterFromRecord(writeRec, 2)
	require.NoError(t, err)
	assert.NotNil(t, bf)
	assert.True(t, bf.Filter.TestString("hello"))
	assert.True(t, bf.Filter.TestString("baz"))
	assert.False(t, bf.Filter.TestString("unknown"))

	// Test BuildBloomFilterFromRecord with invalid column index
	_, err = writer.BuildBloomFilterFromRecord(writeRec, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid params")

	// Test BuildBloomFilterFromRecord with nil record
	_, err = writer.BuildBloomFilterFromRecord(nil, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid params")
}

func TestUniversalBloomFilterWriter_BuildNumberBloomFilter(t *testing.T) {
	pathName := t.TempDir()
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter(pathName, "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	// Create test record
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)
	writeRec.ColVals[0].AppendIntegers(1, 2, 3)

	// Test BuildUniversalBloomFilter
	indexDir := path.Join(pathName, "index")
	err := writer.BuildUniversalBloomFilter(writeRec, 0, indexDir)
	assert.NoError(t, err)

	// Verify that the writer has data (we can't access private fields directly)
	assert.NotNil(t, writer)

	// Test with nil record
	err = writer.BuildUniversalBloomFilter(nil, 0, indexDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record cannot be nil")
}

func TestUniversalBloomFilterReader_ReInit(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewUniversalBloomFilterReader(rpnExpr, schema, option)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Test ReInit with string
	err = reader.ReInit("test_file")
	assert.ErrorContains(t, err, "file need be tssp type")

	mockFile := mockTsspFileBF{path: "/data/data/db0/0...columnstore/mst_00000001-0000-00000000.tssp"}
	err = reader.ReInit(mockFile)
	assert.NoError(t, err)
}

// Test ReInit with TsspFile (mock)
type mockTsspFileBF struct {
	sparseindex.TsspFile
	path string
}

func (m mockTsspFileBF) Path() string {
	return m.path
}

func TestUniversalBloomFilterReader_ErrorCases(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewUniversalBloomFilterReader(rpnExpr, schema, option)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Test MayBeInFragment with invalid fragment ID
	_, err = reader.MayBeInFragment(1000)
	assert.ErrorContains(t, err, "cannot read index file")
}

func TestUniversalBloomFilterReader_GetRowCount(t *testing.T) {
	reader := getBFReader(t)

	// Test GetRowCount
	rowCount, err := reader.GetRowCount(0, &rpn.SKRPNElement{Key: "value", Op: influxql.EQ, Value: int64(2)})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowCount)
}

func TestUniversalBloomFilterWriter_ErrorCases(t *testing.T) {
	writer := sparseindex.NewUniversalGeneralBloomFilterWriter("/test", "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer.Close()

	// Create test record with invalid column type
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Boolean}, // Unsupported type
	}, false)
	writeRec.ColVals[0].AppendBooleans(true, false)

	// Test with unsupported column type
	schemaIdx := []int{0}
	rowsPerSegment := []int{2}
	err := writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported column type")

	// Test with empty record
	emptyRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)
	err = writer.CreateAttachIndex(emptyRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err) // Should handle empty records gracefully
}

func TestUniversalBloomFilter_DefaultFalseRate(t *testing.T) {
	// Test with default false rate
	writer1 := sparseindex.NewUniversalGeneralBloomFilterWriter("/test", "msName", "dataFile", "lockPath", "tokens", &influxql.IndexParam{})
	defer writer1.Close()
	// Test with default false rate - we can't access private fields directly, so we just verify the writer is created
	assert.NotNil(t, writer1)

	// Test with custom false rate
	params := &influxql.IndexParam{IList: []influxql.Expr{&influxql.NumberLiteral{Val: sparseindex.DefaultFalseRate}}}
	writer2 := sparseindex.NewUniversalGeneralBloomFilterWriter("/test", "msName", "dataFile", "lockPath", "tokens", params)
	defer writer2.Close()
	assert.NotNil(t, writer2)
}
