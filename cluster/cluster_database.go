package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"rewriteRedis/config"
	"rewriteRedis/database"
	databaseface "rewriteRedis/interface/database"
	"rewriteRedis/interface/resp"
	"rewriteRedis/lib/consistenthash"
	"rewriteRedis/lib/logger"
	"rewriteRedis/resp/reply"
	"runtime/debug"
	"strings"
)

var router = makeRouter()

type CmdFunc func(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply

// ClusterDatabase 集群层数据库,其实就是一个redis节点
type ClusterDatabase struct {
	self           string                      // 自己的地址
	peers          []string                    // 集群中所有节点地址
	peerPicker     *consistenthash.NodeMap     // 一致性哈希管理器,ClusterDatabase最先收到解析器解析后的指令,需要判断指令去往哪个节点
	peerConnection map[string]*pool.ObjectPool // 节点之间要实现转发功能,A节点转发给B节点需要作为一个B节点的客户端
	database       databaseface.Database       // 下层单机版redis
}

func NewClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
		database:       database.NewDatabase(),
	}
	nodes := make([]string, len(config.Properties.Peers)+1, len(config.Properties.Peers)+1)
	index := 0
	ctx := context.Background()
	for ; index < len(config.Properties.Peers); index++ {
		// 记录集群中所有的节点
		nodes[index] = config.Properties.Peers[index]
		// 对其他节点初始化连接池
		cluster.peerConnection[config.Properties.Peers[index]] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			peer: config.Properties.Peers[index],
		})
	}
	nodes[index] = config.Properties.Self
	cluster.peers = nodes
	// 往一致性哈希管理器中添加节点
	cluster.peerPicker.AddNode(nodes...)
	return cluster
}

// type CmdFunc func(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply

// Exec 开启集群模式后,解析器解析的指令会发送到这里,set key val
func (c *ClusterDatabase) Exec(respConn resp.Connection, cmdMsg [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = reply.NewUnKnowErrReply()
		}
	}()
	cmdName := strings.ToUpper(string(cmdMsg[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.NewStanderErrReply(fmt.Sprintf("ERR Unknown command %s", cmdName))
	}
	result = cmdFunc(c, respConn, cmdMsg)
	return
}

func (c *ClusterDatabase) Close() {
	c.database.Close()
}

func (c *ClusterDatabase) AfterClientClose(respConn resp.Connection) {
	c.database.AfterClientClose(respConn)
}
