package database

import (
	"rewriteRedis/interface/resp"
	"rewriteRedis/lib/logger"
	"rewriteRedis/resp/reply"
)

type EchoDatabse struct {
}

func NewEchoDatabase() *EchoDatabse {
	return &EchoDatabse{}
}

func (e EchoDatabse) Exec(respConn resp.Connection, msg [][]byte) resp.Reply {
	return reply.NewMultiBulkReply(msg)
}

func (e EchoDatabse) Close() {
	//TODO implement me
	logger.Info("EchoDatabase Close")
}

func (e EchoDatabse) AfterClientClose(respConn resp.Connection) {
	//TODO implement me
	logger.Info("EchoDatabase AfterClientClose")
}
