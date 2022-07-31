package database

import (
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
)

func Ping(db *DB, msg [][]byte) resp.Reply {
	return reply.NewPongReply()
}

func init() {
	RegisterCommand("PING", Ping, 1)
}
