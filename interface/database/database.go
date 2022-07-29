package database

import "rewriteRedis/interface/resp"

type Database interface {
	Exec(respConn resp.Connection, msg [][]byte) resp.Reply
	Close()
	AfterClientClose(respConn resp.Connection)
}
