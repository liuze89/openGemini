package metaclient

import (
    "testing"
    "time"
    
    "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// 测试基础查询方法
func TestClients_Databases(t *testing.T) {
    // 准备模拟客户端
    client1 := &MockClient{
        DatabasesFn: func() map[string]*meta.DatabaseInfo {
            return map[string]*meta.DatabaseInfo{"db1": {}, "db2": {}}
        },
    }
    
    client2 := &MockClient{
        DatabasesFn: func() map[string]*meta.DatabaseInfo {
            return map[string]*meta.DatabaseInfo{"db2": {}, "db3": {}}
        },
    }

    clients := Clients{client1, client2}
    
    // 执行并验证
    result := clients.Databases()
    assert.Len(t, result, 3, "应该合并三个数据库")
    assert.Contains(t, result, "db1", "缺少db1")
    assert.Contains(t, result, "db2", "缺少db2")
    assert.Contains(t, result, "db3", "缺少db3")
}

// 测试错误处理逻辑
func TestClients_Database_ErrorHandling(t *testing.T) {
    t.Run("全部客户端失败", func(t *testing.T) {
        client := &MockClient{
            DatabaseFn: func(name string) (*meta.DatabaseInfo, error) {
                return nil, meta.ErrDatabaseNotFound(name)
            },
        }
        
        _, err := Clients{client, client}.Database("testdb")
        assert.ErrorIs(t, err, meta.ErrDatabaseNotFound("testdb"))
    })
    
    t.Run("部分客户端成功", func(t *testing.T) {
        client1 := &MockClient{
            DatabaseFn: func(name string) (*meta.DatabaseInfo, error) {
                return nil, meta.ErrDatabaseNotFound(name)
            },
        }
        client2 := &MockClient{
            DatabaseFn: func(name string) (*meta.DatabaseInfo, error) {
                return &meta.DatabaseInfo{}, nil
            },
        }
        
        _, err := Clients{client1, client2}.Database("testdb")
        assert.NoError(t, err, "应该成功返回")
    })
}

// 测试写操作统一错误
func TestClients_WriteOperations(t *testing.T) {
    tests := []struct {
        name string
        fn   func(*Clients) error
    }{
        {"CreateUser", func(c *Clients) error { 
            _, err := c.CreateUser("test", "pass", false, false)
            return err
        }},
        {"MarkDatabaseDelete", func(c *Clients) error { 
            return c.MarkDatabaseDelete("db") 
        }},
    }

    clients := Clients{&MockClient{}}
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.fn(&clients)
            assert.ErrorIs(t, err, ErrNotImplemented)
        })
    }
}

// 测试资源管理方法
func TestClients_Close(t *testing.T) {
    t.Run("聚合多个错误", func(t *testing.T) {
        client1 := &MockClient{CloseFn: func() error { return assert.AnError }}
        client2 := &MockClient{CloseFn: func() error { return nil }}
        client3 := &MockClient{CloseFn: func() error { return assert.AnError }}

        err := Clients{client1, client2, client3}.Close()
        require.Error(t, err)
        assert.Contains(t, err.Error(), "multiple errors occurred")
        assert.Contains(t, err.Error(), assert.AnError.Error())
    })
}

// MockClient实现（部分）
type MockClient struct {
    DatabasesFn  func() map[string]*meta.DatabaseInfo
    DatabaseFn   func(string) (*meta.DatabaseInfo, error)
    CloseFn      func() error
}

func (m *MockClient) Databases() map[string]*meta.DatabaseInfo {
    if m.DatabasesFn != nil {
        return m.DatabasesFn()
    }
    return nil
}

func (m *MockClient) Database(name string) (*meta.DatabaseInfo, error) {
    if m.DatabaseFn != nil {
        return m.DatabaseFn(name)
    }
    return nil, meta.ErrDatabaseNotFound(name)
}

func (m *MockClient) Close() error {
    if m.CloseFn != nil {
        return m.CloseFn()
    }
    return nil
}

// 测试节点状态查询（带ID偏移）
func TestClients_DataNodes(t *testing.T) {
    client1 := &MockClient{
        DataNodesFn: func() ([]meta.DataNode, error) {
            return []meta.DataNode{
                {ID: 10, Host: "host1"}, 
                {ID: 20, Host: "host2"},
            }, nil
        },
    }
    client2 := &MockClient{
        DataNodesFn: func() ([]meta.DataNode, error) {
            return []meta.DataNode{
                {ID: 30, Host: "host3"},
            }, nil
        },
    }

    nodes, err := Clients{client1, client2}.DataNodes()
    assert.NoError(t, err)
    expected := []struct {
        ID   uint64
        Host string
    }{
        {10, "host1"},  // client1 offset 0
        {20, "host2"},  // client1
        {330, "host3"}, // client2 offset 300 (30+300)
    }
    
    assert.Len(t, nodes, 3)
    for i, n := range nodes {
        assert.Equal(t, expected[i].ID, n.ID)
        assert.Equal(t, expected[i].Host, n.Host)
    }
}

// 测试带偏移的节点查询
func TestClients_DataNode_WithOffset(t *testing.T) {
    client1 := &MockClient{
        DataNodeFn: func(id uint64) (*meta.DataNode, error) {
            return &meta.DataNode{ID: id, Host: "orig-host"}, nil
        },
    }
    client2 := &MockClient{
        DataNodeFn: func(id uint64) (*meta.DataNode, error) {
            return &meta.DataNode{ID: id, Host: "orig-host2"}, nil
        },
    }

    // 测试client1的节点(偏移0)
    node, err := Clients{client1, client2}.DataNode(25) // 25 = 25 + 0*300
    assert.NoError(t, err)
    assert.Equal(t, uint64(25), node.ID)
    assert.Equal(t, "orig-host", node.Host)

    // 测试client2的节点(偏移300)
    node, err = Clients{client1, client2}.DataNode(325) // 325 = 25 + 1*300
    assert.NoError(t, err)
    assert.Equal(t, uint64(25), node.ID)
    assert.Equal(t, "orig-host2", node.Host)

    // 测试无效客户端索引
    _, err = Clients{client1}.DataNode(350) // 350/300=1(超出索引)
    assert.ErrorIs(t, err, meta.ErrNodeNotFound)
}

// 补充MetaNodes偏移测试
func TestClients_MetaNodes_WithOffset(t *testing.T) {
    client1 := &MockClient{
        MetaNodesFn: func() ([]meta.NodeInfo, error) {
            return []meta.NodeInfo{{ID: 1}, {ID: 2}}, nil
        },
    }
    client2 := &MockClient{
        MetaNodesFn: func() ([]meta.NodeInfo, error) {
            return []meta.NodeInfo{{ID: 3}}, nil
        },
    }

    nodes, _ := Clients{client1, client2}.MetaNodes()
    expectedIDs := []uint64{1, 2, 303} // client2偏移300 (3+300)
    assert.Len(t, nodes, 3)
    for i, n := range nodes {
        assert.Equal(t, expectedIDs[i], n.ID)
    }
}

// 补充GetNodePtsMap偏移测试
func TestClients_GetNodePtsMap_WithOffset(t *testing.T) {
    client1 := &MockClient{
        GetNodePtsMapFn: func(db string) (map[uint64][]uint32, error) {
            return map[uint64][]uint32{10: {1,2}}, nil
        },
    }
    client2 := &MockClient{
        GetNodePtsMapFn: func(db string) (map[uint64][]uint32, error) {
            return map[uint64][]uint32{20: {3}}, nil
        },
    }

    result, _ := Clients{client1, client2}.GetNodePtsMap("db")
    assert.Contains(t, result, uint64(10))    // client1无偏移
    assert.Contains(t, result, uint64(320))  // client2偏移300 (20+300)
    assert.ElementsMatch(t, []uint32{1,2}, result[10])
    assert.ElementsMatch(t, []uint32{3}, result[320])
}

// 补充ShowClusterWithCondition反向ID测试
func TestClients_ShowClusterWithCondition(t *testing.T) {
    client1 := &MockClient{
        ShowClusterWithConditionFn: func(typ string, id uint64) (models.Rows, error) {
            return models.Rows{{Name: "node1"}}, nil
        },
    }
    client2 := &MockClient{
        ShowClusterWithConditionFn: func(typ string, id uint64) (models.Rows, error) {
            return models.Rows{{Name: "node2"}}, nil
        },
    }

    // 测试ID 325对应client2的25 (325-300)
    rows, _ := Clients{client1, client2}.ShowClusterWithCondition("datanode", 325)
    assert.Len(t, rows, 1)
    assert.Equal(t, "node2", rows[0].Name)
}

// 扩展MockClient实现
type MockClient struct {
    DatabasesFn  func() map[string]*meta.DatabaseInfo
    DatabaseFn   func(string) (*meta.DatabaseInfo, error)
    CloseFn      func() error
    DataNodesFn          func() ([]meta.DataNode, error)
    ShardsByTimeRangeFn  func(influxql.Sources, time.Time, time.Time) ([]meta.ShardInfo, error)
    AuthenticateFn       func(string, string) (meta.User, error)
    AcquireLockFn            func(string, time.Duration) error
    GetLockInfoFn           func(string) (meta.LockInfo, error)
    ShowContinuousQueriesFn func() (models.Rows, error)
    ShowDownSamplePoliciesFn func(string) (models.Rows, error)
}

func (m *MockClient) AcquireLock(name string, timeout time.Duration) error {
    if m.AcquireLockFn != nil {
        return m.AcquireLockFn(name, timeout)
    }
    return ErrNotImplemented
}

func (m *MockClient) GetLockInfo(name string) (meta.LockInfo, error) {
    if m.GetLockInfoFn != nil {
        return m.GetLockInfoFn(name)
    }
    return meta.LockInfo{}, nil
}

func (m *MockClient) ShowContinuousQueries() (models.Rows, error) {
    if m.ShowContinuousQueriesFn != nil {
        return m.ShowContinuousQueriesFn()
    }
    return nil, nil
}

func (m *MockClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
    if m.ShowDownSamplePoliciesFn != nil {
        return m.ShowDownSamplePoliciesFn(database)
    }
    return nil, nil
}

// 测试热分片映射
func TestClients_ThermalShards(t *testing.T) {
    client1 := &MockClient{
        ThermalShardsFn: func(db string, s, e time.Duration) map[uint64]struct{} {
            return map[uint64]struct{}{100: {}, 200: {}}
        },
    }
    client2 := &MockClient{
        ThermalShardsFn: func(db string, s, e time.Duration) map[uint64]struct{} {
            return map[uint64]struct{}{300: {}}
        },
    }

    result := Clients{client1, client2}.ThermalShards("db", 0, 0)
    expected := map[uint64]struct{}{
        100: {},  // client1 offset 0
        200: {},
        600: {},  // client2 offset 300 (300+300)
    }
    assert.Equal(t, expected, result)
}

// 测试流策略方法
func TestClients_StreamMethods(t *testing.T) {
    t.Run("CreateStreamPolicy返回错误", func(t *testing.T) {
        clients := Clients{&MockClient{}}
        err := clients.CreateStreamPolicy(&meta2.StreamInfo{})
        assert.ErrorIs(t, err, ErrNotImplemented)
    })

    t.Run("合并流策略信息", func(t *testing.T) {
        info1 := &meta2.StreamInfo{Name: "stream1"}
        info2 := &meta2.StreamInfo{Name: "stream2"}
        
        client1 := &MockClient{GetStreamInfosFn: func() map[string]*meta2.StreamInfo {
            return map[string]*meta2.StreamInfo{"stream1": info1}
        }}
        client2 := &MockClient{GetStreamInfosFn: func() map[string]*meta2.StreamInfo {
            return map[string]*meta2.StreamInfo{"stream2": info2}
        }}

        result := Clients{client1, client2}.GetStreamInfos()
        assert.Len(t, result, 2)
        assert.Contains(t, result, "stream1")
        assert.Contains(t, result, "stream2")
    })
}

// 测试副本组查询
func TestClients_DBRepGroups(t *testing.T) {
    group1 := meta2.ReplicaGroup{ID: 1, MasterID: 100}
    group2 := meta2.ReplicaGroup{ID: 2, MasterID: 200}

    client1 := &MockClient{
        DBRepGroupsFn: func(db string) []meta2.ReplicaGroup {
            return []meta2.ReplicaGroup{group1}
        },
    }
    client2 := &MockClient{
        DBRepGroupsFn: func(db string) []meta2.ReplicaGroup {
            return []meta2.ReplicaGroup{group2}
        },
    }

    groups := Clients{client1, client2}.DBRepGroups("db")
    assert.Len(t, groups, 2)
    assert.Contains(t, groups, group1)
    assert.Contains(t, groups, group2)
}

// 测试SQLite启用状态
func TestClients_IsSQLiteEnabled(t *testing.T) {
    t.Run("全部客户端启用", func(t *testing.T) {
        client1 := &MockClient{IsSQLiteEnabledFn: func() bool { return true }}
        client2 := &MockClient{IsSQLiteEnabledFn: func() bool { return true }}
        assert.True(t, Clients{client1, client2}.IsSQLiteEnabled())
    })

    t.Run("部分客户端未启用", func(t *testing.T) {
        client1 := &MockClient{IsSQLiteEnabledFn: func() bool { return true }}
        client2 := &MockClient{IsSQLiteEnabledFn: func() bool { return false }}
        assert.False(t, Clients{client1, client2}.IsSQLiteEnabled())
    })
}

// 扩展MockClient实现
type MockClient struct {
    DatabasesFn  func() map[string]*meta.DatabaseInfo
    DatabaseFn   func(string) (*meta.DatabaseInfo, error)
    CloseFn      func() error
    DataNodesFn          func() ([]meta.DataNode, error)
    ShardsByTimeRangeFn  func(influxql.Sources, time.Time, time.Time) ([]meta.ShardInfo, error)
    AuthenticateFn       func(string, string) (meta.User, error)
    AcquireLockFn            func(string, time.Duration) error
    GetLockInfoFn           func(string) (meta.LockInfo, error)
    ShowContinuousQueriesFn func() (models.Rows, error)
    ShowDownSamplePoliciesFn func(string) (models.Rows, error)
}

func (m *MockClient) AcquireLock(name string, timeout time.Duration) error {
    if m.AcquireLockFn != nil {
        return m.AcquireLockFn(name, timeout)
    }
    return ErrNotImplemented
}

func (m *MockClient) GetLockInfo(name string) (meta.LockInfo, error) {
    if m.GetLockInfoFn != nil {
        return m.GetLockInfoFn(name)
    }
    return meta.LockInfo{}, nil
}

func (m *MockClient) ShowContinuousQueries() (models.Rows, error) {
    if m.ShowContinuousQueriesFn != nil {
        return m.ShowContinuousQueriesFn()
    }
    return nil, nil
}

func (m *MockClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
    if m.ShowDownSamplePoliciesFn != nil {
        return m.ShowDownSamplePoliciesFn(database)
    }
    return nil, nil
}

// 测试分片管理方法
func TestClients_ShardManagement(t *testing.T) {
    t.Run("UpdateShardInfoTier返回错误", func(t *testing.T) {
        clients := Clients{&MockClient{}}
        err := clients.UpdateShardInfoTier(100, 1, "db", "rp")
        assert.ErrorIs(t, err, ErrNotImplemented)
    })

    t.Run("ShardOwner合并结果", func(t *testing.T) {
        client1 := &MockClient{
            ShardOwnerFn: func(id uint64) (string, string, *meta2.ShardGroupInfo) {
                return "db1", "rp1", &meta2.ShardGroupInfo{ID: 100}
            },
        }
        client2 := &MockClient{
            ShardOwnerFn: func(id uint64) (string, string, *meta2.ShardGroupInfo) {
                return "", "", nil
            },
        }

        db, rp, info := Clients{client1, client2}.ShardOwner(200)
        assert.Equal(t, "db1", db)
        assert.Equal(t, "rp1", rp)
        assert.Equal(t, uint64(100), info.ID)
    })
}

// 测试用户权限管理
func TestClients_UserPrivileges(t *testing.T) {
    clients := Clients{&MockClient{}}
    
    t.Run("SetAdminPrivilege返回错误", func(t *testing.T) {
        err := clients.SetAdminPrivilege("user", true)
        assert.ErrorIs(t, err, ErrNotImplemented)
    })
}

// 测试测量点管理
func TestClients_MeasurementManagement(t *testing.T) {
    t.Run("CreateMeasurement返回错误", func(t *testing.T) {
        clients := Clients{&MockClient{}}
        _, err := clients.CreateMeasurement("db", "rp", "mst", nil, 0, nil, 0, nil, nil, nil)
        assert.ErrorIs(t, err, ErrNotImplemented)
    })

    t.Run("GetMeasurements合并结果", func(t *testing.T) {
        m1 := &meta2.MeasurementInfo{Name: "cpu"}
        m2 := &meta2.MeasurementInfo{Name: "mem"}
        
        client1 := &MockClient{
            GetMeasurementsFn: func(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
                return []*meta2.MeasurementInfo{m1}, nil
            },
        }
        client2 := &MockClient{
            GetMeasurementsFn: func(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
                return []*meta2.MeasurementInfo{m2}, nil
            },
        }

        result, _ := Clients{client1, client2}.GetMeasurements(nil)
        assert.Len(t, result, 2)
        assert.Contains(t, result, m1)
        assert.Contains(t, result, m2)
    })
}

// 测试系统控制方法
func TestClients_SystemControl(t *testing.T) {
    clients := Clients{&MockClient{}}
    
    t.Run("SendSysCtrlToMeta返回错误", func(t *testing.T) {
        _, err := clients.SendSysCtrlToMeta("clean", nil)
        assert.ErrorIs(t, err, ErrNotImplemented)
    })
}

// 测试文件操作方法
func TestClients_FileOperations(t *testing.T) {
    clients := Clients{&MockClient{}}
    
    t.Run("InsertFiles返回错误", func(t *testing.T) {
        err := clients.InsertFiles([]meta2.FileInfo{{}})
        assert.ErrorIs(t, err, ErrNotImplemented)
    })
}

// 扩展MockClient实现
type MockClient struct {
    UpdateShardInfoTierFn func(uint64, uint64, string, string) error
    ShardOwnerFn         func(uint64) (string, string, *meta2.ShardGroupInfo)
    SetAdminPrivilegeFn  func(string, bool) error
    CreateMeasurementFn func(string, string, string, *meta2.ShardKeyInfo, int32, *influxql.IndexRelation, config.EngineType, *meta2.ColStoreInfo, []*proto2.FieldSchema, *meta2.Options) (*meta2.MeasurementInfo, error)
    GetMeasurementsFn   func(*influxql.Measurement) ([]*meta2.MeasurementInfo, error)
    SendSysCtrlToMetaFn func(string, map[string]string) (map[string]string, error)
    InsertFilesFn       func([]meta2.FileInfo) error
}

func (m *MockClient) UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error {
    if m.UpdateShardInfoTierFn != nil {
        return m.UpdateShardInfoTierFn(shardID, tier, dbName, rpName)
    }
    return ErrNotImplemented
}

func (m *MockClient) ShardOwner(id uint64) (string, string, *meta2.ShardGroupInfo) {
    if m.ShardOwnerFn != nil {
        return m.ShardOwnerFn(id)
    }
    return "", "", nil
}

func (m *MockClient) SetAdminPrivilege(username string, admin bool) error {
    if m.SetAdminPrivilegeFn != nil {
        return m.SetAdminPrivilegeFn(username, admin)
    }
    return ErrNotImplemented
}

func (m *MockClient) CreateMeasurement(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation, engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
    if m.CreateMeasurementFn != nil {
        return m.CreateMeasurementFn(database, retentionPolicy, mst, shardKey, numOfShards, indexR, engineType, colStoreInfo, schemaInfo, options)
    }
    return nil, ErrNotImplemented
}

func (m *MockClient) GetMeasurements(meas *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
    if m.GetMeasurementsFn != nil {
        return m.GetMeasurementsFn(meas)
    }
    return nil, nil
}

func (m *MockClient) SendSysCtrlToMeta(mod string, param map[string]string) (map[string]string, error) {
    if m.SendSysCtrlToMetaFn != nil {
        return m.SendSysCtrlToMetaFn(mod, param)
    }
    return nil, ErrNotImplemented
}

func (m *MockClient) InsertFiles(files []meta2.FileInfo) error {
    if m.InsertFilesFn != nil {
        return m.InsertFilesFn(files)
    }
    return ErrNotImplemented
}