package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"rewriteRedis/resp/client"
)

// 连接工厂,集群中节点之间的转发需要借助连接池
type connectionFactory struct {
	peer string //对某一个节点的连接
}

// MakeObject 创建客户端连接
func (cf *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	clientObj, err := client.MakeClient(cf.peer)
	if err != nil {
		return nil, err
	}
	clientObj.Start()
	return pool.NewPooledObject(clientObj), nil
}

// DestroyObject 销毁客户端连接
func (cf *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	clientObj, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("wrong type client")
	}
	// 关闭连接
	clientObj.Close()
	return nil
}

func (cf *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (cf *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (cf *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
