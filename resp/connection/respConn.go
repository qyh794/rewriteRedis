package connection

import (
	"net"
	"rewriteRedis/lib/sync/wait"
	"sync"
	"time"
)

// 对net.conn 的封装
type RespConnection struct {
	conn net.Conn
	// 等待回复结束
	waitingReply wait.Wait
	// 回复客户端时加锁
	mu sync.Mutex
	// 选择的分数据库
	selectedDB int
}

func (r *RespConnection) Close() error {
	r.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = r.conn.Close()
	return nil
}

func (r *RespConnection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	r.mu.Lock()
	r.waitingReply.Add(1)
	defer r.waitingReply.Done()
	defer r.mu.Unlock()
	// 使用resp封装的net.conn回复
	_, err := r.conn.Write(bytes)
	return err
}

func (r *RespConnection) GetDBIndex() int {
	return r.selectedDB
}

func (r *RespConnection) SelectDB(i int) {
	r.selectedDB = i
}

func (r *RespConnection) RemoteAddr() net.Addr {
	return r.conn.RemoteAddr()
}

func NewRespConnection(conn net.Conn) *RespConnection {
	return &RespConnection{
		conn: conn,
	}
}
