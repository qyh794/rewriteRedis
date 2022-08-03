package database

import (
	"rewriteRedis/interface/resp"
	"rewriteRedis/lib/utils"
	"rewriteRedis/resp/reply"
)

// execDel del key1, key2...指令执行函数
func execDel(db *DB, msg [][]byte) resp.Reply {
	keys := make([]string, len(msg))
	for i, v := range msg {
		keys[i] = string(v)
	}
	deleted := db.RemoveKeys(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("DEL", msg...))
	}
	return reply.NewIntReply(int64(deleted))
}

// execExists exists key1, key2...指令执行函数,判断key是否存在
func execExists(db *DB, msg [][]byte) resp.Reply {
	res := int64(0)
	for _, v := range msg {
		key := string(v)
		_, exists := db.GetEntity(key)
		if exists {
			res++
		}
	}
	return reply.NewIntReply(res)
}

// execFlushDB 清空当前数据库
func execFlushDB(db *DB, msg [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("FLUSHDB", msg...))
	return reply.NewOKReply()
}

// execType Type KEY执行执行函数
func execType(db *DB, msg [][]byte) resp.Reply {
	key := string(msg[0])
	val, exists := db.GetEntity(key)
	if !exists {
		return reply.NewStatusReply("none")
	}
	switch val.(type) {
	case []byte:
		return reply.NewStatusReply("string")
		// 后续实现list、hash
	}
	return reply.NewUnKnowErrReply()
}

// execRename rename oldKey newKey
func execRename(db *DB, msg [][]byte) resp.Reply {
	old := string(msg[0])
	oldVal, exists := db.GetEntity(old)
	if !exists {
		return reply.NewStanderErrReply("ERR no such key")
	}
	newKey := string(msg[1])
	db.Remove(old)
	db.SetEntity(newKey, oldVal)
	db.addAof(utils.ToCmdLine2("RENAME", msg...))
	return reply.NewOKReply()
}

// execRenameNx RenameNx oldKey newKey,当newKey不存在时,将oldKey改名为newKey
func execRenameNx(db *DB, msg [][]byte) resp.Reply {
	old := string(msg[0])
	val, exists := db.GetEntity(old)
	if !exists {
		return reply.NewStanderErrReply("ERR no such key")
	}
	newKey := string(msg[1])
	_, exists = db.GetEntity(newKey)
	if exists {
		return reply.NewIntReply(int64(0))
	}
	db.Remove(old)
	db.SetEntity(newKey, val)
	db.addAof(utils.ToCmdLine2("RENAMENX", msg...))
	return reply.NewIntReply(int64(1))
}

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, msg [][]byte) resp.Reply {
	temp := db.KeysAll()
	res := make([][]byte, 0)
	for i := 0; i < len(temp); i++ {
		res = append(res, []byte(temp[i]))
	}
	return reply.NewMultiBulkReply(res)
}

func init() {
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("EXISTS", execExists, -2)
	RegisterCommand("FLUSHDB", execFlushDB, -1)
	RegisterCommand("TYPE", execType, 2)
	RegisterCommand("RENAME", execRename, 3)
	RegisterCommand("RENAMENX", execRenameNx, 3)
	RegisterCommand("KEYS", execKeys, 2)

}
