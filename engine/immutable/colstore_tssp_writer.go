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

package immutable

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type ColumnStoreWriteResult struct {
	File          TSSPFile
	Fragments     []int64
	PKRecord      *record.Record
	IndexFilePath string
}

type ColumnStoreTSSPWriter struct {
	sw  *StreamWriteFile
	mst *colstore.Measurement
	pkf colstore.PrimaryKeyFetcher

	timeCol record.ColVal
	oks     colstore.OffsetKeySorter

	iw *ColStoreIndexWriter
}

func NewColumnStoreTSSPWriter(sw *StreamWriteFile, mst *colstore.Measurement) *ColumnStoreTSSPWriter {
	return &ColumnStoreTSSPWriter{
		sw:  sw,
		mst: mst,
	}
}

func (b *ColumnStoreTSSPWriter) WriteRecord(rec *record.Record) (*ColumnStoreWriteResult, error) {
	record.CheckRecord(rec)

	ir := b.mst.IndexRelation()

	b.iw = NewColStoreIndexWriter(b.sw.dir, b.sw.name, b.sw.fileName, b.sw.lock, ir, rec.Schema)
	defer b.iw.MustClose()

	var ret = &ColumnStoreWriteResult{}
	var oks *colstore.OffsetKeySorter

	// 1. group by primary key
	// 2. sort groups by primary key
	// 3. sort within groups by sort key or time
	ret.PKRecord, ret.Fragments, oks = b.sortRecord(rec)

	err := b.writeRecord(rec, oks)
	if err != nil {
		return nil, err
	}

	err = b.iw.Flush()
	if err != nil {
		return nil, err
	}

	file, err := b.sw.NewTSSPFile(true)
	if err != nil {
		return nil, err
	}

	file.SetSkipIndexInfo(b.iw.BuildSkipIndexInfo())
	ret.File = file

	err = b.writePrimaryIndex(ret)
	if err != nil {
		return nil, err
	}

	return ret, err
}

func (b *ColumnStoreTSSPWriter) sortRecord(rec *record.Record) (*record.Record, []int64, *colstore.OffsetKeySorter) {
	pk := b.mst.PrimaryKey()
	sk := b.mst.SortKey()

	pkRec, offsetsMap, pkSlice := b.pkf.Fetch(rec, pk, b.mst.TCDuration())

	timeCol := &b.timeCol
	oks := &b.oks
	oksSwap := &colstore.OffsetKeySorter{}

	var fragments []int64
	var segCount int64 = 0
	var buf []byte

	skCols := record.FetchColVals(rec, sk)

	// Sort the data within each group by the sort key
	for i := range pkSlice {
		timeCol.Init()
		oksSwap.Reset()

		oksSwap.Offsets = offsetsMap.AppendToIfExists(util.Bytes2str(pkSlice[i]), oksSwap.Offsets[:0])
		pickColVal(rec.TimeColumn(), timeCol, oksSwap.Offsets, influx.Field_Type_Int)
		oksSwap.Times = append(oksSwap.Times[:0], timeCol.IntegerValues()...)

		// If the sort key contains only a time column, sort by time
		if len(sk) != 1 || sk[0].Name != record.TimeField {
			for _, n := range oksSwap.Offsets {
				buf = colstore.FetchKeyAtRow(buf, skCols, sk, int(n), 0)
				oksSwap.Add(buf)
				buf = buf[len(buf):]
			}
		}

		sort.Sort(oksSwap)
		oks.Append(oksSwap)
		segCount += int64(util.DivisionCeil(oksSwap.Len(), GetColStoreConfig().GetMaxRowsPerSegment()))

		// A fragment contains one or more segments, record the offset of the last segment.
		fragments = append(fragments, segCount)
	}

	return pkRec, fragments, oks
}

func (b *ColumnStoreTSSPWriter) writeRecord(rec *record.Record, oks *colstore.OffsetKeySorter) error {
	sw := b.sw
	sw.ChangeSid(colstore.SeriesID)
	var err error

	pkMap := b.mst.BuildPKMap()
	picker := colstore.NewColValPicker(oks, b.mst.IsUniqueEnabled())
	maxRowsPreSeg := GetColStoreConfig().GetMaxRowsPerSegment()

	for i := range rec.Schema.Len() - 1 { // skip time column
		ref := rec.Schema[i]
		if _, ok := pkMap[ref.Name]; ok {
			continue
		}

		column := rec.Column(i)
		if err = sw.AppendColumn(&ref); err != nil {
			return err
		}

		picker.IteratorSegment(maxRowsPreSeg, func(start, end int) bool {
			dataCol, timeCol := picker.Pick(column, ref.Type, start, end)

			err = sw.WriteData(colstore.SeriesID, ref, *dataCol, timeCol)
			if err != nil {
				return false
			}

			err = b.iw.Write(ref, dataCol)
			return err == nil
		})

		if err != nil {
			return err
		}
	}

	ref := rec.LastSchema()
	if err = sw.AppendColumn(&ref); err != nil {
		return err
	}
	picker.IteratorSegment(maxRowsPreSeg, func(start, end int) bool {
		_, timeCol := picker.Pick(rec.TimeColumn(), ref.Type, start, end)

		err = sw.WriteData(colstore.SeriesID, ref, *timeCol, timeCol)

		return err == nil
	})

	if err != nil {
		return err
	}

	return b.sw.WriteCurrentMeta()
}

func (b *ColumnStoreTSSPWriter) writePrimaryIndex(ret *ColumnStoreWriteResult) error {
	indexFilePath := BuildPKFilePathFromTSSP(ret.File.Path())
	indexBuilder := colstore.NewIndexBuilder(b.sw.lock, indexFilePath+GetTmpFileSuffix())
	ret.IndexFilePath = indexFilePath

	defer indexBuilder.Reset()
	AppendFragmentsToPKRecord(ret.PKRecord, ret.Fragments)
	return indexBuilder.WriteData(ret.PKRecord, b.mst.TCLocation())
}

type ColStoreIndexWriter struct {
	ir           *influxql.IndexRelation
	indexBuilder *index.IndexWriterBuilder
	indexWriters map[string][]index.IndexWriter
}

func NewColStoreIndexWriter(dir, mst string, fileName TSSPFileName, lock *string,
	ir *influxql.IndexRelation, schema record.Schemas) *ColStoreIndexWriter {

	iw := &ColStoreIndexWriter{
		ir: ir,
	}
	iw.indexBuilder = index.NewIndexWriterBuilder()
	iw.indexWriters = iw.indexBuilder.NewIndexWriters(dir, mst, fileName.String(), *lock, schema, *ir)
	iw.Open()
	return iw
}

func (iw *ColStoreIndexWriter) MustClose() {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		util.MustClose(writer)
	}
}

func (iw *ColStoreIndexWriter) Flush() error {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (iw *ColStoreIndexWriter) Open() {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		writer.Open()
	}
}

func (iw *ColStoreIndexWriter) Write(ref record.Field, col *record.ColVal) error {
	if len(iw.indexWriters) == 0 {
		return nil
	}

	skipWriters, ok := iw.indexWriters[ref.Name]
	if !ok {
		return nil
	}

	var err error

	rec := &record.Record{}
	rec.Schema = append(rec.Schema, ref)
	rec.ColVals = append(rec.ColVals, *col)
	for i := range skipWriters {
		err = skipWriters[i].CreateAttachIndex(rec, []int{0}, []int{col.Len})
		if err != nil {
			return err
		}
	}
	return nil
}

func (iw *ColStoreIndexWriter) BuildSkipIndexInfo() []*colstore.SkipIndexInfo {
	return iw.indexBuilder.BuildSkipIndexInfo()
}

func pickColVal(src *record.ColVal, dst *record.ColVal, ofs []int64, typ int) {
	for _, i := range ofs {
		dst.AppendColVal(src, typ, int(i), int(i+1))
	}
}

func AppendFragmentsToPKRecord(dst *record.Record, fragments []int64) {
	dst.ReserveSchema(1)
	dst.ReserveColVal(1)

	dst.Schema[dst.Len()-1].Name = record.FragmentField
	dst.Schema[dst.Len()-1].Type = influx.Field_Type_Int
	dst.ColVals[dst.Len()-1].AppendIntegers(fragments...)
}
