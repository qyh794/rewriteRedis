package handler

import (
	"context"
	"io"
	"net"
	databaseface "rewriteRedis/interface/database"
	"rewriteRedis/lib/logger"
	"rewriteRedis/lib/sync/atomic"
	"rewriteRedis/resp/connection"
	"rewriteRedis/resp/parser"
	"rewriteRedis/resp/reply"
	"sync"
)

var unKnownErrReply = []byte("-Err unknown\r\n")

type respHandler struct {
	activeConn sync.Map // 活跃的连接
	db         databaseface.Database
	closing    atomic.Boolean
}

func NewHandler() *respHandler {
	return &respHandler{}
}

func (r respHandler) closeConn(respConn *connection.RespConnection) {
	_ = respConn.Close()
	r.activeConn.Delete(respConn)
}

func (r respHandler) Handle(ctx context.Context, conn net.Conn) {
	// 新建一个resp连接
	respConn := connection.NewRespConnection(conn)
	// 保存连接
	r.activeConn.Store(respConn, struct{}{})
	// 处理resp.conn中发来的消息
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			// 如果是EOF,需要关闭客户端连接
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF {
				r.closeConn(respConn)
				logger.Info("resp connection closed: " + respConn.RemoteAddr().String())
				return
			}
			// 协议错误
			errReply := reply.NewStanderErrReply(payload.Err.Error())
			// 直接返回给客户端
			err := respConn.Write(errReply.ToByte())
			// 返回出错
			if err != nil {
				r.closeConn(respConn)
				logger.Info("connection closed: " + respConn.RemoteAddr().String())
				return
			}
			// 回写成功
			continue
		} else {
			// 数据为空
			if payload.Data == nil {
				logger.Error("empty payload")
				continue
			}
			// 有数据
			bulkReply := payload.Data.(*reply.MultiBulkReply)
			res := r.db.Exec(respConn, bulkReply.Msg)
			if res != nil {
				_ = respConn.Write(res.ToByte())
			} else {
				_ = respConn.Write(unKnownErrReply)
			}
		}
	}
}

func (r respHandler) Close() error {
	//TODO implement me
	panic("implement me")
}
