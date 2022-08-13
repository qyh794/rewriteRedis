package cluster

import (
	"context"
	"errors"
	"rewriteRedis/interface/resp"
	"rewriteRedis/lib/utils"
	"rewriteRedis/resp/client"
	"rewriteRedis/resp/reply"
	"strconv"
)

// getNodeClient 获取目标节点的连接
func (c *ClusterDatabase) getNodeClient(node string) (*client.Client, error) {
	// ClusterDatabase维护了一个对各个节点的连接池,需要从连接池中获取连接
	pool, ok := c.peerConnection[node]
	if !ok {
		return nil, errors.New("connection not found")
	}
	// 从连接池中获取一个连接
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, errors.New("something wrong with getNodeClient")
	}
	cli, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("this is not a NodeClient")
	}
	return cli, nil
}

// returnNodeClient 归还连接到连接池中
func (c *ClusterDatabase) returnNodeClient(node string, nodeClient *client.Client) error {
	// 归还到哪个连接池中
	pool, ok := c.peerConnection[node]
	if !ok {
		return errors.New("connection not found in peerConnection")
	}
	return pool.ReturnObject(context.Background(), nodeClient)
}

// relay 转发指令
func (c *ClusterDatabase) relay(node string, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	// 判断是不是自己,如果是直接发送到下层的database
	if node == c.self {
		return c.database.Exec(respConn, cmdMsg)
	}
	// 需要转发给别的节点,从连接池中获取一个对该节点的连接(客户端)
	nodeClient, err := c.getNodeClient(node)
	if err != nil {
		return reply.NewStanderErrReply(err.Error())
	}
	defer func() {
		c.returnNodeClient(node, nodeClient)
	}()
	// 通过连接将指令发送至目标节点
	nodeClient.Send(utils.ToCmdLine("select", strconv.Itoa(respConn.GetDBIndex())))
	return nodeClient.Send(cmdMsg)
}
