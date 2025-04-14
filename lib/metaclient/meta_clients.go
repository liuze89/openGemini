package metaclient

import (
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influxql"
)

// Clients 实现MetaClient接口的客户端集合
type Clients []*Client

// 查询类方法实现（示例）
func (c Clients) Databases() map[string]*meta.DatabaseInfo {
	// 合并所有客户端的数据库信息
	result := make(map[string]*meta.DatabaseInfo)
	for _, client := range c {
		for k, v := range client.Databases() {
			result[k] = v
		}
	}
	return result
}

func (c Clients) Database(name string) (*meta.DatabaseInfo, error) {
	// 遍历所有客户端查询
	for _, client := range c {
		if db, err := client.Database(name); err == nil {
			return db, nil
		}
	}
	return nil, meta.ErrDatabaseNotFound(name)
}

// 修改类方法实现（空实现）
func (c Clients) CreateUser(name, password string, admin, rwuser bool) (meta.User, error) {
	return nil, ErrNotImplemented
}

func (c Clients) UpdateUser(name, password string) error {
	return ErrNotImplemented
}

func (c Clients) DropUser(name string) error {
	return ErrNotImplemented
}

// 实现所有接口方法（部分示例）...
func (c Clients) CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta.DatabaseInfo, error) {
	return nil, ErrNotImplemented
}

var ErrNotImplemented = errors.New("write operations not supported in clients collection")

// 查询类方法实现
func (c Clients) TagKeys(database string) map[string]set.Set {
	result := make(map[string]set.Set)
	for _, client := range c {
		for k, v := range client.TagKeys(database) {
			if _, ok := result[k]; !ok {
				result[k] = v
			}
		}
	}
	return result
}

func (c Clients) GetMeasurementID(database, rpName, mstName string) (uint64, error) {
	for _, client := range c {
		if id, err := client.GetMeasurementID(database, rpName, mstName); err == nil {
			return id, nil
		}
	}
	return 0, meta2.ErrMeasurementNotFound
}

// 合并多个客户端的存储节点信息（带ID偏移）
func (c Clients) DataNodes() ([]meta2.DataNode, error) {
	var nodes []meta2.DataNode
	for i, client := range c {
		offset := uint64(i * 300)
		dn, err := client.DataNodes()
		if err != nil {
			continue
		}
		// 为每个节点的ID添加客户端偏移量
		for _, n := range dn {
			nodes = append(nodes, meta2.DataNode{
				ID:      n.ID + offset,
				Host:    n.Host,
				TCPHost: n.TCPHost,
			})
		}
	}
	return nodes, nil
}

// 实现节点状态查询（带ID偏移解析）
func (c Clients) DataNode(id uint64) (*meta2.DataNode, error) {
	clientIndex := int(id / 300)
	originalID := id % 300
	
	// 检查客户端索引有效性
	if clientIndex < 0 || clientIndex >= len(c) {
		return nil, meta2.ErrNodeNotFound
	}
	
	// 获取原始客户端查询结果
	node, err := c[clientIndex].DataNode(originalID)
	if err != nil {
		return nil, err
	}
	
	// 返回带偏移量的节点信息
	return &meta2.DataNode{
		ID:      node.ID + uint64(clientIndex*300),
		Host:    node.Host,
		TCPHost: node.TCPHost,
	}, nil
}

// 标记保留策略删除
func (c Clients) MarkRetentionPolicyDelete(database, name string) error {
    return ErrNotImplemented
}

// 标记测量点删除
func (c Clients) MarkMeasurementDelete(database, policy, measurement string) error {
    return ErrNotImplemented
}

// 存储引擎启动
func (c Clients) OpenAtStore() error {
    return ErrNotImplemented
}

// 带条件的集群展示
func (c Clients) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
    var rows models.Rows
    for _, client := range c {
        if r, err := client.ShowClusterWithCondition(nodeType, ID); err == nil {
            rows = append(rows, r...)
        }
    }
    return rows, nil
}

// 分片层级更新
func (c Clients) UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error {
    return ErrNotImplemented
}

// 获取带过滤的测量点信息
func (c Clients) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta2.RpMeasurementsFieldsInfo, error) {
    var result meta2.RpMeasurementsFieldsInfo
    for _, client := range c {
        if info, err := client.GetMstInfoWithInRp(dbName, rpName, dataTypes); err == nil {
            return info, nil
        }
    }
    return &result, nil
}

// 备份恢复操作（已存在但需确认）
func (c Clients) SendBackupToMeta(mod string, param map[string]string, host string) (map[string]string, error) {
    return nil, ErrNotImplemented
}

// 数据库选项查询
func (c Clients) DatabaseOption(name string) (*obs.ObsOptions, error) {
    for _, client := range c {
        if opt, err := client.DatabaseOption(name); err == nil {
            return opt, nil
        }
    }
    return nil, meta2.ErrDatabaseNotFound(name)
}

func (c Clients) Measurement(database, rpName, mstName string) (*meta2.MeasurementInfo, error) {
	for _, client := range c {
		if m, err := client.Measurement(database, rpName, mstName); err == nil {
			return m, nil
		}
	}
	return nil, meta2.ErrMeasurementNotFound
}

func (c Clients) ShowShards(database, rp, mst string) models.Rows {
	var rows models.Rows
	for _, client := range c {
		rows = append(rows, client.ShowShards(database, rp, mst)...)
	}
	return rows
}

// 特殊处理：需要同步所有客户端的方法
func (c Clients) Open() error {
	for _, client := range c {
		if err := client.Open(); err != nil {
			return err
		}
	}
	return nil
}

// 实现所有接口方法...
// [注：此处应完整实现MetaClient接口的约60个方法，模式类似上述示例]

// 实现流处理相关方法
func (c Clients) CreateStreamPolicy(info *meta2.StreamInfo) error {
	return ErrNotImplemented
}

func (c Clients) GetStreamInfos() map[string]*meta2.StreamInfo {
	result := make(map[string]*meta2.StreamInfo)
	for _, client := range c {
		for k, v := range client.GetStreamInfos() {
			result[k] = v
		}
	}
	return result
}

// 实现分片查询方法 
func (c Clients) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) ([]meta2.ShardInfo, error) {
	var shards []meta2.ShardInfo
	seen := make(map[uint64]struct{})
	
	for _, client := range c {
		result, err := client.ShardsByTimeRange(sources, tmin, tmax)
		if err != nil {
			continue
		}
		for _, s := range result {
			if _, ok := seen[s.ID]; !ok {
				shards = append(shards, s)
				seen[s.ID] = struct{}{}
			}
		}
	}
	return shards, nil
}

// 实现节点状态查询
func (c Clients) DataNode(id uint64) (*meta2.DataNode, error) {
	for _, client := range c {
		if node, err := client.DataNode(id); err == nil {
			return node, nil
		}
	}
	return nil, meta2.ErrNodeNotFound
}

// 实现权限管理方法
func (c Clients) SetAdminPrivilege(username string, admin bool) error {
	return ErrNotImplemented
}

// 实现连续查询方法
func (c Clients) CreateContinuousQuery(database, name, query string) error {
	return ErrNotImplemented
}

// 实现关闭方法
func (c Clients) Close() error {
	var errs []error
	for _, client := range c {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple errors occurred: %v", errs)
	}
	return nil
}

// 实现元节点查询
func (c Clients) MetaNodes() ([]meta2.NodeInfo, error) {
	var nodes []meta2.NodeInfo
	seen := make(map[uint64]struct{})
	
	for _, client := range c {
		if result, err := client.MetaNodes(); err == nil {
			for _, n := range result {
				if _, ok := seen[n.ID]; !ok {
					nodes = append(nodes, n)
					seen[n.ID] = struct{}{}
				}
			}
		}
	}
	return nodes, nil
}

// 实现降采样策略方法
func (c Clients) NewDownSamplePolicy(database, name string, info *meta2.DownSamplePolicyInfo) error {
    return ErrNotImplemented
}

func (c Clients) DropDownSamplePolicy(database, name string, dropAll bool) error {
    return ErrNotImplemented
}

func (c Clients) ShowDownSamplePolicies(database string) (models.Rows, error) {
    var rows models.Rows
    seen := make(map[string]bool)
    for _, client := range c {
        if r, err := client.ShowDownSamplePolicies(database); err == nil {
            for _, row := range r {
                if !seen[row.Name] {
                    rows = append(rows, row)
                    seen[row.Name] = true
                }
            }
        }
    }
    return rows, nil
}

// 实现热分片管理
// 修改ThermalShards方法
func (c Clients) ThermalShards(db string, start, end time.Duration) map[uint64]struct{} {
    result := make(map[uint64]struct{})
    for i, client := range c {
        offset := uint64(i * 300)
        for shard := range client.ThermalShards(db, start, end) {
            result[shard + offset] = struct{}{} // 添加偏移量到分片ID
        }
    }
    return result
}

// 修改GetNodePtsMap方法
func (c Clients) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
    result := make(map[uint64][]uint32)
    for i, client := range c {
        offset := uint64(i * 300)
        if m, err := client.GetNodePtsMap(database); err == nil {
            for k, v := range m {
                result[k + offset] = append(result[k + offset], v...) // 添加偏移量到节点ID
            }
        }
    }
    return result, nil
}

// 修改MetaNodes方法（添加ID偏移）
func (c Clients) MetaNodes() ([]meta2.NodeInfo, error) {
    var nodes []meta2.NodeInfo
    for i, client := range c {
        offset := uint64(i * 300)
        if result, err := client.MetaNodes(); err == nil {
            for _, n := range result {
                nodes = append(nodes, meta2.NodeInfo{
                    ID:      n.ID + offset, // 添加偏移量
                    Host:    n.Host,
                    TCPHost: n.TCPHost,
                })
            }
        }
    }
    return nodes, nil
}

// 修改ShowClusterWithCondition方法
func (c Clients) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
    var rows models.Rows
    for i, client := range c {
        offset := uint64(i * 300)
        if r, err := client.ShowClusterWithCondition(nodeType, ID - offset); err == nil { // 反向计算原始ID
            rows = append(rows, r...)
        }
    }
    return rows, nil
}

// 实现用户认证方法
func (c Clients) Authenticate(username, password string) (meta2.User, error) {
    var lastErr error
    for _, client := range c {
        if user, err := client.Authenticate(username, password); err == nil {
            return user, nil
        } else {
            lastErr = err
        }
    }
    return nil, lastErr
}

// 实现系统控制方法
func (c Clients) SendSysCtrlToMeta(mod string, param map[string]string) (map[string]string, error) {
    return nil, ErrNotImplemented
}

// 实现SQL心跳方法
func (c Clients) SendSql2MetaHeartbeat(host string) error {
    var errs []error
    for _, client := range c {
        if err := client.SendSql2MetaHeartbeat(host); err != nil {
            errs = append(errs, err)
        }
    }
    if len(errs) > 0 {
        return fmt.Errorf("heartbeat errors: %v", errs)
    }
    return nil
}

// 实现副本组查询
func (c Clients) DBRepGroups(database string) []meta2.ReplicaGroup {
    var groups []meta2.ReplicaGroup
    seen := make(map[uint32]bool)
    for _, client := range c {
        for _, g := range client.DBRepGroups(database) {
            if !seen[g.ID] {
                groups = append(groups, g)
                seen[g.ID] = true
            }
        }
    }
    return groups
}

// 实现文件操作方法
func (c Clients) InsertFiles(files []meta2.FileInfo) error {
    return ErrNotImplemented
}

func (c Clients) IsSQLiteEnabled() bool {
    // 所有客户端都启用时才返回true
    for _, client := range c {
        if !client.IsSQLiteEnabled() {
            return false
        }
    }
    return true
}

// 实现持续查询展示
func (c Clients) ShowContinuousQueries() (models.Rows, error) {
    var rows models.Rows
    seen := make(map[string]bool)
    for _, client := range c {
        if r, err := client.ShowContinuousQueries(); err == nil {
            for _, row := range r {
                key := row.Name + "@" + row.Database
                if !seen[key] {
                    rows = append(rows, row)
                    seen[key] = true
                }
            }
        }
    }
    return rows, nil
}

// 实现副本数查询
func (c Clients) GetReplicaN(database string) (int, error) {
    for _, client := range c {
        if n, err := client.GetReplicaN(database); err == nil {
            return n, nil
        }
    }
    return 0, meta2.ErrDatabaseNotFound(database)
}

// 实现分片所有者查询
func (c Clients) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
    for _, client := range c {
        db, rp, info := client.ShardOwner(shardID)
        if info != nil {
            return db, rp, info
        }
    }
    return "", "", nil
}

// 实现管理接口检查
func (c Clients) IsMasterPt(ptID uint32, db string) bool {
    // 任意客户端是主分片即返回true
    for _, client := range c {
        if client.IsMasterPt(ptID, db) {
            return true
        }
    }
    return false
}

// 实现测量点管理
func (c Clients) CreateMeasurement(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo, 
    numOfShards int32, indexR *influxql.IndexRelation, engineType config.EngineType,
    colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
    return nil, ErrNotImplemented
}

func (c Clients) GetMeasurements(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
    var measurements []*meta2.MeasurementInfo
    seen := make(map[string]bool)
    for _, client := range c {
        if mInfos, err := client.GetMeasurements(m); err == nil {
            for _, info := range mInfos {
                key := info.Name + "@" + info.Database + "#" + info.RetentionPolicy
                if !seen[key] {
                    measurements = append(measurements, info)
                    seen[key] = true
                }
            }
        }
    }
    if len(measurements) == 0 {
        return nil, meta2.ErrMeasurementNotFound
    }
    return measurements, nil
}

// 实现索引管理
func (c Clients) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo) error {
    return ErrNotImplemented
}

func (c Clients) CreateIndex(database, retentionPolicy, mst string, indexInfo *meta2.IndexInfo) error {
    return ErrNotImplemented
}

// 实现schema查询
func (c Clients) Schema(database string, retentionPolicy string, mst string) (map[string]int32, map[string]struct{}, error) {
    var (
        fields    = make(map[string]int32)
        dimensions = make(map[string]struct{})
    )
    
    for _, client := range c {
        f, d, err := client.Schema(database, retentionPolicy, mst)
        if err != nil {
            continue
        }
        for k, v := range f {
            if _, ok := fields[k]; !ok {
                fields[k] = v
            }
        }
        for k := range d {
            if _, ok := dimensions[k]; !ok {
                dimensions[k] = struct{}{}
            }
        }
    }
    
    if len(fields) == 0 && len(dimensions) == 0 {
        return nil, nil, meta2.ErrMeasurementNotFound
    }
    return fields, dimensions, nil
}

// 实现保留策略查询
func (c Clients) RetentionPolicy(database, name string) (*meta2.RetentionPolicyInfo, error) {
    for _, client := range c {
        if rp, err := client.RetentionPolicy(database, name); err == nil {
            return rp, nil
        }
    }
    return nil, meta2.ErrRetentionPolicyNotFound(name)
}

// 集群状态展示方法
func (c Clients) ShowCluster(nodeType string, ID uint64) (models.Rows, error) {
    var rows models.Rows
    for _, client := range c {
        if r, err := client.ShowCluster(nodeType, ID); err == nil {
            rows = append(rows, r...)
        }
    }
    return rows, nil
}

// 分片迁移状态跟踪
func (c Clients) UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error {
    return ErrNotImplemented
}

// 管理用户存在检查
func (c Clients) AdminUserExists() bool {
    for _, client := range c {
        if client.AdminUserExists() {
            return true
        }
    }
    return false
}

// 查询ID注册
func (c Clients) RetryRegisterQueryIDOffset(host string) (uint64, error) {
    for _, client := range c {
        if id, err := client.RetryRegisterQueryIDOffset(host); err == nil {
            return id, nil
        }
    }
    return 0, errors.New("all clients failed")
}

// 节点分区映射
func (c Clients) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
    result := make(map[uint64][]uint32)
    for _, client := range c {
        if m, err := client.GetNodePtsMap(database); err == nil {
            for k, v := range m {
                result[k] = append(result[k], v...)
            }
        }
    }
    return result, nil
}