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

package compactor

import (
	"errors"
	"fmt"
	"io"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type CompactContext struct {
	dec       codec.BinaryDecoder
	swap      *record.Record
	fragments []int64
}

var compactCtxPool = pool.NewDefaultUnionPool(func() *CompactContext {
	return &CompactContext{
		swap: &record.Record{},
	}
})

func InitColStoreCompactor() {
	if !config.GetStoreConfig().ColumnStore.CompactEnabled {
		return
	}

	immutable.SetColStoreCompactorFactory(func(tables *immutable.MmsTables, mst string) immutable.ColStoreCompactor {
		return &ColStoreCompactor{
			fileLock: tables.GetLockPath(),
			sw:       tables.NewStreamWriteFile(mst),
		}
	})
}

type ColStoreCompactor struct {
	mst      *colstore.Measurement
	sw       *immutable.StreamWriteFile
	files    []immutable.TSSPFile
	fileLock *string
	iw       *immutable.ColStoreIndexWriter
}

func (c *ColStoreCompactor) createIteratorBuilder(files []immutable.TSSPFile) (*immutable.FragmentIteratorBuilder, error) {
	its := make([]*immutable.CSFileIterator, len(files))
	for i, file := range files {
		it, err := immutable.NewCSFileIterator(file, c.mst.SortKey(), c.mst.TCDuration())
		if err != nil {
			return nil, err
		}
		its[i] = it
	}

	return immutable.NewFragmentIteratorBuilder(its, c.mst.SortKey()), nil
}

func (c *ColStoreCompactor) initWriter(file immutable.TSSPFile) error {
	if err := c.sw.InitCompactFile(file); err != nil {
		return err
	}

	return nil
}

func (c *ColStoreCompactor) Compact(ident colstore.MeasurementIdent, files []immutable.TSSPFile) ([]immutable.TSSPFile, error) {
	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		return nil, fmt.Errorf("mst not exists")
	}
	c.mst = mst

	if err := c.initWriter(files[0]); err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			c.sw.Clean()
		}
	}()

	c.iw = immutable.NewColStoreIndexWriter(c.sw.GetDir(), ident.Name, c.sw.GetFileName(), c.fileLock, mst.IndexRelation(), record.Schemas{})
	defer c.iw.MustClose()

	err := c.compact(files)

	success = err == nil
	return c.files, err
}

func (c *ColStoreCompactor) compact(files []immutable.TSSPFile) error {
	batchSize := immutable.GetColStoreConfig().GetMaxRowsPerSegment()
	builder, err := c.createIteratorBuilder(files)
	if err != nil {
		return err
	}

	pki := files[0].GetPkInfo()
	ctx := compactCtxPool.Get()
	pkRec := &record.Record{}
	schema := pki.GetRec().Schema
	pkRec.ResetWithSchema(schema[:schema.Len()-1])
	dec := &ctx.dec
	rec := ctx.swap
	fragments := ctx.fragments[:0]

	defer func() {
		ctx.fragments = fragments
		compactCtxPool.PutWithMemSize(ctx, 0)
	}()

	var segCount int64 = 0
	for {
		itr, pkBuf, err := builder.BuildNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		colstore.BuildPkRecord(dec, pkRec, pkBuf)

		for {
			err = itr.AppendTo(rec, batchSize)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			err = c.WriteSegment(rec)
			segCount++
			if err != nil {
				return err
			}
		}
		fragments = append(fragments, segCount)
	}

	err = CheckPKRecord(pkRec)
	if err == nil {
		err = c.newTsspFile(pkRec, fragments)
	}

	return err
}

func (c *ColStoreCompactor) newTsspFile(pkRec *record.Record, fragments []int64) error {
	err := c.sw.WriteCurrentMeta()
	if err != nil {
		return err
	}

	file, err := c.sw.NewTSSPFile(true)
	if err != nil {
		return err
	}
	if file == nil {
		return fmt.Errorf("compactor file is nil")
	}
	c.files = append(c.files, file)

	err = c.writePrimaryIndex(file.Path(), pkRec, fragments)
	if err != nil {
		return err
	}

	uts := util.Bytes2Uint64Slice(util.Int64Slice2byte(fragments))
	pkMark := fragment.NewIndexFragmentVariable(uts)
	file.SetPkInfo(colstore.NewPKInfo(pkRec, pkMark, c.mst.ColStoreInfo().GetPKType(), -1))
	file.SetSkipIndexInfo(c.iw.BuildSkipIndexInfo())

	return nil
}

func (c *ColStoreCompactor) writePrimaryIndex(path string, pkRec *record.Record, fragments []int64) error {
	indexFilePath := immutable.BuildPKFilePathFromTSSP(path)
	indexBuilder := colstore.NewIndexBuilder(c.fileLock, indexFilePath+immutable.GetTmpFileSuffix())

	defer indexBuilder.Reset()
	immutable.AppendFragmentsToPKRecord(pkRec, fragments)
	return indexBuilder.WriteData(pkRec, c.mst.TCLocation())
}

func (c *ColStoreCompactor) WriteSegment(rec *record.Record) error {
	timeCol := rec.TimeColumn()
	var err error

	if c.sw.Size() == 0 {
		c.sw.Init(colstore.SeriesID, rec.Schema)
	}

	for i := range rec.Schema.Len() {
		ref := rec.Schema[i]
		err = c.sw.ChangeColumn(ref)
		if err != nil {
			return err
		}

		err = c.sw.WriteData(colstore.SeriesID, ref, rec.ColVals[i], timeCol)
		if err != nil {
			return err
		}

		err = c.iw.Write(ref, &rec.ColVals[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func CheckPKRecord(rec *record.Record) error {
	for i := range rec.Len() {
		col := &rec.ColVals[i]
		if col.NilCount > 0 {
			return errors.New("[BUG] record contains null values")
		}
		if col.Len == 0 {
			return errors.New("[BUG] record contains no values")
		}
	}
	return nil
}
