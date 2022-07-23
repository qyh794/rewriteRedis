package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"rewriteRedis/lib/logger"
	"rewriteRedis/lib/sync/atomic"
	"rewriteRedis/lib/sync/wait"
	"sync"
	"time"
)

// // 业务逻辑处理,当tcp层接收新连接后交由Handler处理这个连接
// type Handler interface {
// 	Handle(ctx context.Context, conn net.Conn)
// 	Close() error
// }

type EchoRedisConnection struct {
	conn net.Conn
	waiting wait.Wait
}

type EchoRedisHandler struct {
	activeConn sync.Map
	closing atomic.Boolean
}

func NewEchoRedisHandler() *EchoRedisHandler {
	return &EchoRedisHandler{}
}

func (e *EchoRedisConnection) Close()  {
	e.waiting.WaitWithTimeout(10 * time.Second)
	err := e.conn.Close()
	if err != nil {
		logger.Error("something wrong with conn.Close()")
	}
}

// 处理用户连接
func (e *EchoRedisHandler) Handle(ctx context.Context, conn net.Conn) {
	if e.closing.Get() {
		_ = conn.Close()
	}
	redisConnection := &EchoRedisConnection{
		conn: conn,
	}
	e.activeConn.Store(redisConnection, struct{}{})
	// 从conn中接收数据
	br := bufio.NewReader(conn)
	for {
		// 以\n作为结束符读取一行数据
		msg, err := br.ReadString('\n')
		if err != nil {
			// 如果是操作系统中的结束符
			if err == io.EOF {
				logger.Info("connection close")
				// 删除记录中活跃的redis连接
				e.activeConn.Delete(redisConnection)
			} else {
				logger.Warn("something wrong with ReadString(), err:", err)
			}
			return
		}
		
		// 加锁
		redisConnection.waiting.Add(1)
		// 回写数据, 原样返回数据
		b := []byte(msg)
		_, err = conn.Write(b)
		if err != nil {
			logger.Error("something wrong with conn.Write(), err: ", err)
			return
		}
		logger.Info("conn.Writer success")
		redisConnection.waiting.Done()
	}
}

// 关闭处理器
func (e *EchoRedisHandler) Close() error {
	logger.Info("handler shutting down...")
	e.closing.Set(true)
	// 关闭连接
	e.activeConn.Range(func(key, value any) bool {
		erc := key.(*EchoRedisConnection)
		erc.Close()
		return true
	})
	return nil
}