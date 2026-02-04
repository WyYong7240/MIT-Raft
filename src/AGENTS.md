# MIT 6.5840 Raft 实现项目

## 项目概述

这是一个 MIT 6.5840 分布式系统课程的 Raft 共识算法实现项目。项目使用 Go 语言实现，包含 Raft 协议的核心实现以及基于 Raft 的键值存储系统和分片键值存储系统。

### 主要组件

- **raft1**: Raft 共识算法的核心实现
  - `raft.go`: Raft 协议主要逻辑，包含状态机、日志复制、选举等
  - `raft_test.go`: Raft 协议测试套件（3A-3D 部分）
  - `server.go`: Raft 服务器实现
  - `util.go`: 调试工具和日志系统

- **kvraft1**: 基于 Raft 的键值存储系统
  - `server.go`: KV 服务器实现
  - `client.go`: KV 客户端实现
  - `rsm/`: 可复制状态机（Replicated State Machine）

- **shardkv1**: 分片键值存储系统
  - `shardctrler/`: 分片控制器
  - `shardgrp/`: 分片组管理
  - `shardcfg/`: 分片配置

- **kvsrv1**: 基础键值服务器
  - `rpc/`: RPC 定义
  - `lock/`: 锁服务实现

- **tester1**: 测试框架
  - 提供测试配置、服务器管理、网络故障模拟等功能

- **labrpc**: 自定义 RPC 库，支持网络延迟和故障模拟
- **labgob**: 序列化库

## 技术栈

- **语言**: Go 1.24.0
- **依赖**: 
  - `github.com/anishathalye/porcupine v1.0.3` (线性一致性验证)
  - `github.com/gofrs/flock v0.13.0` (文件锁)
  - `golang.org/x/sys v0.37.0`

## 构建和运行

### 运行测试

#### Raft 测试 (raft1)

```bash
# 运行所有 Raft 测试
cd raft1
go test -v

# 运行特定测试
go test -v -run TestInitialElection3A
go test -v -run TestBasicAgree3B
go test -v -run TestCount3B

# 设置调试级别
VERBOSE=1 go test -v
```

**测试分类**:
- **3A 测试**: 领导选举
  - `TestInitialElection3A`: 初始选举
  - `TestReElection3A`: 重新选举
  - `TestManyElections3A`: 多次选举

- **3B 测试**: 日志复制
  - `TestBasicAgree3B`: 基本日志一致
  - `TestRPCBytes3B`: RPC 字节数验证
  - `TestFollowerFailure3B`: 跟随者故障
  - `TestLeaderFailure3B`: 领导者故障
  - `TestFailAgree3B`: 故障时的一致性
  - `TestFailNoAgree3B`: 故障时不一致
  - `TestConcurrentStarts3B`: 并发启动
  - `TestRejoin3B`: 服务器重新加入
  - `TestBackup3B`: 备份测试
  - `TestCount3B`: 计数测试

- **3C 测试**: 持久化和故障恢复
  - `TestPersist13C`: 持久化测试 1
  - `TestPersist23C`: 持久化测试 2
  - `TestPersist33C`: 持久化测试 3
  - `TestFigure83C`: Figure 8 场景
  - `TestUnreliableAgree3C`: 不可靠网络一致性
  - `TestFigure8Unreliable3C`: Figure 8 不可靠场景
  - `TestReliableChurn3C`: 可靠的动态变化
  - `TestUnreliableChurn3C`: 不可靠的动态变化

- **3D 测试**: 快照
  - `TestSnapshotBasic3D`: 基本快照
  - `TestSnapshotInstall3D`: 快照安装
  - `TestSnapshotInstallUnreliable3D`: 不可靠环境快照安装
  - `TestSnapshotInstallCrash3D`: 快照安装崩溃
  - `TestSnapshotInstallUnCrash3D`: 快照安装不崩溃
  - `TestSnapshotAllCrash3D`: 所有节点崩溃快照
  - `TestSnapshotInit3D`: 快照初始化

#### KV 测试 (kvraft1)

```bash
cd kvraft1
go test -v
```

#### 分片 KV 测试 (shardkv1)

```bash
cd shardkv1
go test -v
```

### 编译

```bash
# 在项目根目录编译
go build ./raft1
go build ./kvraft1
go build ./shardkv1
```

## 项目结构

```
src/
├── raft1/           # Raft 核心实现
│   ├── raft.go      # Raft 协议主文件
│   ├── raft_test.go # Raft 测试套件
│   └── util.go      # 调试工具
├── kvraft1/         # 基于 Raft 的 KV 存储
│   ├── server.go    # KV 服务器
│   ├── client.go    # KV 客户端
│   └── rsm/         # 可复制状态机
├── shardkv1/        # 分片 KV 存储
│   ├── shardctrler/ # 分片控制器
│   ├── shardgrp/    # 分片组
│   └── shardcfg/    # 分片配置
├── kvsrv1/          # 基础 KV 服务器
│   ├── rpc/         # RPC 定义
│   └── lock/        # 锁服务
├── tester1/         # 测试框架
├── labrpc/          # 自定义 RPC 库
├── labgob/          # 序列化库
└── raftapi/         # Raft API 定义
```

## 开发约定

### 代码风格

1. **导入顺序**: 标准库 -> 第三方库 -> 项目内部包
2. **命名约定**: 
   - 公开函数使用 PascalCase
   - 私有函数使用 camelCase
   - 常量使用 UPPER_CASE
3. **并发控制**: 使用 `sync.Mutex` 保护共享状态
4. **错误处理**: 使用 Go 的标准错误处理模式

### 调试

项目使用自定义的调试系统，通过 `VERBOSE` 环境变量控制输出级别：

```bash
# 不显示调试信息
go test -v

# 显示调试信息
VERBOSE=1 go test -v

# 显示更详细的调试信息
VERBOSE=2 go test -v
```

调试主题包括：
- `dClient`: 客户端相关
- `dCommit`: 提交相关
- `dDrop`: 丢包相关
- `dError`: 错误信息
- `dInfo`: 一般信息
- `dLeader`: 领导者相关
- `dLog`: 日志相关
- `dPersist`: 持久化相关
- `dSnap`: 快照相关
- `dTerm`: 任期相关
- `dVote`: 投票相关

### 测试规范

- 每个测试函数以 `Test` 开头，遵循 Go 测试惯例
- 测试文件命名为 `*_test.go`
- 使用 `defer ts.cleanup()` 确保测试资源清理
- 使用 `ts.Begin()` 标记测试开始

### 关键常量

在 `raft.go` 中定义了关键的超时和调试常量：

- `TIMTOUTDURATION_INTERVAL = 100`: 选举超时基础间隔（毫秒）
- `BASE_TIMEOUT_DURATION = 300`: 基础超时时间（毫秒）
- `SERVER_TIMEOUT = 500`: 服务器超时间隔（毫秒）
- `SERVER_BASE_TIMEOUT = 1000`: 服务器基础超时（毫秒）

## Raft 协议实现要点

### 状态定义

```go
type ServerState int
const (
    FOLLOWER  ServerState = 0
    CANDIDATE ServerState = 1
    LEADER    ServerState = 2
)
```

### 持久化状态

- `CurrentTerm`: 当前任期
- `VoteFor`: 投票给的服务器 ID
- `Log`: 日志条目数组

### 易失性状态

- `CommitIndex`: 已提交的日志索引
- `LastApplied`: 已应用到状态机的日志索引
- `NextIndex[]`: Leader 的发送索引数组
- `MatchIndex[]`: Leader 的匹配索引数组

### 关键方法

- `Make()`: 创建 Raft 实例
- `Start()`: 开始日志复制
- `GetState()`: 获取当前状态
- `Kill()`: 停止 Raft 实例
- `Snapshot()`: 创建快照
- `persist()`: 持久化状态

## 当前状态

根据 Git 日志，项目已完成：
- ✅ 3B 测试全部通过
- ⚠️ 3A 测试可能存在问题
- 🔄 正在优化 3B 测试时间

最近的提交：
- `dca9b99`: optimize 3B Test Time
- `7aadfd4`: Pass All 3B Test But 3A In Trouble
- `f7667bc`: Backup Test sync
- `b883797`: Pass 3B RejoinTest
- `383abcf`: Pass 3B BasicAgree Test

## 注意事项

1. 修改 Raft 实现后，务必运行完整的测试套件
2. 调试时使用 `VERBOSE` 环境变量获取详细日志
3. 注意网络故障模拟和节点崩溃场景
4. 日志文件存储在 `raft1/debugLogs/` 目录中，可用于分析问题
5. 不要修改测试器代码（`tester1/`），只修改实现代码