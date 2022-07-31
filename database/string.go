package database

import (
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
)

// Get指令执行函数
func execGet(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val, exists := db.GetEntity(key)
	if !exists {
		return reply.NewNullBulkReply()
	}
	bytes := val.([]byte)
	return reply.NewBulkReply(bytes)
}

// Set指令执行函数
func execSet(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val := msg[1]
	db.SetEntity(key, val)
	return reply.NewOKReply()
}

// SetNX指令执行函数
func execSetNX(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val := msg[1]
	res := db.SetIfAbsent(key, val)
	return reply.NewIntReply(int64(res))
}

// GetSet执行执行函数,将给定 key 的值设为 value ，并返回 key 的旧值(old value),如果key不存在就set key,返回nil
func execGetSet(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val := msg[1]
	old, exists := db.GetEntity(key)
	db.SetEntity(key, val)
	if !exists {
		return reply.NewNullBulkReply()
	}
	// 返回key的旧值
	return reply.NewBulkReply(old.([]byte))
}

// strLen strLen key执行函数,返回key对应的Value的长度
func execStrLen(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val, exists := db.GetEntity(key)
	if !exists {
		return reply.NewIntReply(int64(0))
	}
	return reply.NewIntReply(int64(len(val.([]byte))))
}

func init() {
	RegisterCommand("GET", execGet, 2)
	RegisterCommand("SET", execSet, -3)
	RegisterCommand("SETNX", execSetNX, 3)
	RegisterCommand("GETSET", execGetSet, 3)
	RegisterCommand("STRLEN", execStrLen, 2)
}
