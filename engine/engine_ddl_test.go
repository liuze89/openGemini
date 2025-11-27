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

package engine

import (
	"io"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

const (
	customPtId    = uint32(2)
	customShardId = uint64(2)
)

func writeNormalData(eng *EngineImpl, mst string) error {
	msNames := []string{mst}
	tm := time.Now().Truncate(time.Second)

	// order
	rows, _, _ := GenDataRecord(msNames, 10, 10, time.Second, tm, false, true, true)
	if err := eng.WriteRows(defaultDb, defaultRp, customPtId, customShardId, rows, nil, nil); err != nil {
		return err
	}
	eng.ForceFlush()

	// unordered
	rows, _, _ = GenDataRecord(msNames, 10, 10, time.Second, tm.Add(-time.Minute), false, true, true)
	if err := eng.WriteRows(defaultDb, defaultRp, customPtId, customShardId, rows, nil, nil); err != nil {
		return err
	}
	eng.ForceFlush()

	dbInfo := eng.DBPartitions[defaultDb][defaultPtId]
	for _, item := range dbInfo.indexBuilder {
		idx, ok := item.GetPrimaryIndex().(*tsi.MergeSetIndex)
		if ok {
			idx.DebugFlush()
		}
	}

	return nil
}

func createShardWithTimeRange(eng *EngineImpl, db string, rp string, ptId uint32, shardId uint64) error {
	eng.CreateDBPT(defaultDb, customPtId, false)
	tr := meta.TimeRangeInfo{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now().Add(time.Hour),
	}
	shardDuration := getShardDurationInfo(shardId)
	shardTimeRange := &meta.ShardTimeRangeInfo{
		TimeRange: tr,
		OwnerIndex: meta.IndexDescriptor{
			IndexID:      shardId,
			IndexGroupID: shardId,
			TimeRange:    tr,
		},
		ShardDuration: shardDuration,
	}

	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}

	return eng.CreateShard(db, rp, ptId, shardId, shardTimeRange, msInfo)
}

func TestShard_CreateRecordIterator(t *testing.T) {
	sh := &shard{
		ident: &meta.ShardIdentifier{},
	}

	opt := &query.ProcessorOptions{StartTime: 100, EndTime: time.Now().Add(time.Hour).UnixNano()}
	itr := sh.CreateConsumeIterator("mst", opt)
	require.Empty(t, itr)

	plan := sh.CreateDDLBasePlan(nil, 1)
	require.Empty(t, plan)
}

func TestShard_CreateCSRecordIterator(t *testing.T) {
	ident := colstore.MeasurementIdent{DB: defaultDb, RP: defaultRp, Name: "mst"}
	mi := &meta.MeasurementInfo{Name: "mst", EngineType: config.COLUMNSTORE}
	schema := make(meta.CleanSchema)
	schema["pk"] = meta.SchemaVal{Typ: influx.Field_Type_Tag}
	schema["sk"] = meta.SchemaVal{Typ: influx.Field_Type_Tag}
	mi.Schema = &schema
	mi.ColStoreInfo = &meta.ColStoreInfo{
		PropertyKey: []string{"pk"},
		SortKey:     []string{"sk"},
	}

	colstore.MstManagerIns().Add(ident, mi)
	defer func() {
		colstore.MstManagerIns().Clear()
	}()

	opt := &query.ProcessorOptions{StartTime: 100, EndTime: time.Now().Add(time.Hour).UnixNano()}

	dir := t.TempDir()
	eng, err := initEngine(dir)
	require.NoError(t, err)
	defer func(eng *EngineImpl) {
		err = eng.Close()
		if err != nil {
			t.Errorf("failed to close engine: %v", err)
		}
	}(eng)

	err = createShardWithTimeRange(eng, defaultDb, defaultRp, customPtId, customShardId)
	require.NoError(t, err)

	itr := eng.CreateConsumeIterator(defaultDb, "mst", opt)
	require.Empty(t, itr)
}

func TestConsumeIterator(t *testing.T) {
	ci := &ConsumeIterator{
		itrs: make([]record.Iterator, 5),
		tms:  []int64{1, 3, 9, 7, 2},
	}
	sort.Sort(ci)
	require.Equal(t, ci.tms, []int64{1, 2, 3, 7, 9})
}

func TestEngineImpl_CreateConsumeIterator(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	require.NoError(t, err)
	defer func(eng *EngineImpl) {
		err = eng.Close()
		if err != nil {
			t.Errorf("failed to close engine: %v", err)
		}
	}(eng)

	err = createShardWithTimeRange(eng, defaultDb, defaultRp, customPtId, customShardId)
	require.NoError(t, err)

	require.NoError(t, writeNormalData(eng, defaultMeasurementName))

	msi := &influxql.Measurement{
		Database:        defaultDb,
		RetentionPolicy: defaultRp,
		Name:            defaultMeasurementName,
	}
	type args struct {
		opt *query.ProcessorOptions
	}

	tests := []struct {
		name             string
		args             args
		condition        string
		expectedRowCount int
		expectedSidCount int
	}{
		{
			name: "case1: single condition testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
					{Val: "field3_bool", Type: influxql.Boolean},
					{Val: "field4_float", Type: influxql.Float},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "tagkey2", Type: influxql.Tag},
					{Val: "tagkey3", Type: influxql.Tag},
					{Val: "tagkey4", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
			}},
			condition:        "tagkey1::tag = 'tagvalue1_1'",
			expectedRowCount: 20,
			expectedSidCount: 1,
		},
		{
			name: "case2: single condition testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field2_int", Type: influxql.Integer},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey2", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
			}},
			condition:        "tagkey1::tag != 'tagvalue1_3'",
			expectedRowCount: 180,
			expectedSidCount: 9,
		},
		{
			name: "case3: single condition testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "tagkey2", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "field2_int", Type: influxql.Integer},
				},
			}},
			condition:        "field2_int::integer < 5",
			expectedRowCount: 20,
			expectedSidCount: 4,
		},
		{
			name: "case4: single condition testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
				},
				Aux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
				},
			}},
			condition:        "field1_string::string = 'test-test-test-test-1'",
			expectedRowCount: 2,
			expectedSidCount: 1,
		},
		{
			name: "case5: multiple tag conditions combined using AND",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
					{Val: "field3_bool", Type: influxql.Boolean},
					{Val: "field4_float", Type: influxql.Float},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey3", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "tagkey2", Type: influxql.Tag},
				},
			}},
			condition:        "tagkey1::tag = 'tagvalue1_1' AND tagkey2::tag != 'tagvalue1_3'",
			expectedRowCount: 20,
			expectedSidCount: 1,
		},
		{
			name: "case6:  multiple field conditions combined using AND",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "field2_int", Type: influxql.Integer},
					{Val: "field3_bool", Type: influxql.Boolean},
					{Val: "field4_float", Type: influxql.Float},
				},
			}},
			condition:        "field2_int::integer > 8 AND field4_float::float < 10 AND field3_bool::boolean = true",
			expectedRowCount: 10,
			expectedSidCount: 5,
		},
		{
			name: "case7: multiple tag and field conditions combined using AND",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "field1_string", Type: influxql.String},
				},
			}},
			condition:        "tagkey1::tag = 'tagvalue1_2' AND field1_string::string = 'test-test-test-test-1'",
			expectedRowCount: 2,
			expectedSidCount: 1,
		},
		{
			name: "case8: multiple tag conditions combined using OR",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "tagkey2", Type: influxql.Tag},
				},
			}},
			condition:        "tagkey1::tag = 'tagvalue1_3' OR tagkey2::tag = 'tagvalue2_2'",
			expectedRowCount: 40,
			expectedSidCount: 2,
		},
		{
			name: "case9: multiple field conditions combined using OR",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
				},
				Aux: []influxql.VarRef{
					{Val: "field2_int", Type: influxql.Integer},
					{Val: "field3_bool", Type: influxql.Boolean},
					{Val: "field4_float", Type: influxql.Float},
				},
			}},
			condition:        "field2_int::integer > 9 OR field4_float::float < 2 OR field3_bool::boolean = true",
			expectedRowCount: 150,
			expectedSidCount: 10,
		},
		{
			name: "case10: multiple tag and field conditions combined using OR",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "field1_string", Type: influxql.String},
				},
			}},
			condition:        "tagkey1::tag = 'tagvalue1_1' OR field1_string::string = 'test-test-test-test-1'",
			expectedRowCount: 22,
			expectedSidCount: 2,
		},
		{
			name: "case11: Unconditional testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
			}},
			expectedRowCount: 200,
			expectedSidCount: 10,
		},
		{
			name: "case12: Conditional Conflict Testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: 0,
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
				},
				Aux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "field1_string", Type: influxql.String},
				},
			}},
			condition:        "tagkey1::tag != 'tagvalue1_2' AND field1_string::string = 'test-test-test-test-1'",
			expectedRowCount: 0,
			expectedSidCount: 0,
		},
		{
			name: "case13: Time Filtering testing",
			args: args{opt: &query.ProcessorOptions{
				StartTime: time.Now().Add(-30 * time.Second).UnixNano(),
				EndTime:   time.Now().Add(time.Hour).UnixNano(),
				FieldAux: []influxql.VarRef{
					{Val: "field1_string", Type: influxql.String},
					{Val: "field2_int", Type: influxql.Integer},
					{Val: "field3_bool", Type: influxql.Boolean},
					{Val: "field4_float", Type: influxql.Float},
				},
				TagAux: []influxql.VarRef{
					{Val: "tagkey1", Type: influxql.Tag},
					{Val: "tagkey2", Type: influxql.Tag},
					{Val: "tagkey3", Type: influxql.Tag},
					{Val: "tagkey4", Type: influxql.Tag},
				},
			}},
			expectedRowCount: 100,
			expectedSidCount: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, _ := influxql.ParseExpr(tt.condition)
			tt.args.opt.Condition = expr

			itrs := eng.CreateConsumeIterator(msi.Database, msi.Name, tt.args.opt)
			require.Equal(t, 1, len(itrs))

			for _, itr := range itrs {
				processIterator(t, itr, tt.expectedRowCount)
			}
			for _, itr := range itrs {
				itr.Release()
			}
		})
	}
}

func processIterator(t *testing.T, itr record.Iterator, expectedRowNums int) {
	rowCount := 0
	rec, err := itr.Next()
	if expectedRowNums == 0 {
		require.Error(t, io.EOF, err)
		return
	}
	require.NoError(t, err)
	for err != io.EOF {
		rowCount += rec.Rec.RowNums()
		rec, err = itr.Next()
	}
	require.Equal(t, expectedRowNums, rowCount)
}
