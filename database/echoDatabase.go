package database

import (
	"rewriteRedis/interface/resp"
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
	panic("implement me")
}

