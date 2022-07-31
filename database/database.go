package database

import (
	"rewriteRedis/config"
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
	"strconv"
	"strings"
)

type database struct {
	dbs []*DB
}

func NewDatabase() *database {
	redisDB := &database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	redisDB.dbs = make([]*DB, config.Properties.Databases)
	for i := range redisDB.dbs {
		singleDB := NewDB()
		singleDB.index = i
		redisDB.dbs[i] = singleDB
	}
	return redisDB
}

// 指令 set key value
func (d *database) Exec(respConn resp.Connection, msg [][]byte) resp.Reply {
	cmdName := strings.ToUpper(string(msg[0]))
	// 如果指令是select 1
	if cmdName == "select" {
		if len(msg) != 2 { // 如果是select
			return reply.NewArgNumErrReply("select")
		}
		return execSelect(respConn, msg, d)
	}
	// 其他指令,需要分数据库去执行
	// 1.获取该指令需要的分数据库
	dbIndex := respConn.GetDBIndex()
	db := d.dbs[dbIndex]
	return db.Exec(msg)
}

func (d *database) Close() {

}

func (d *database) AfterClientClose(respConn resp.Connection) {
	
}

func execSelect(respConn resp.Connection, msg [][]byte, db *database) resp.Reply {
	dbIndex, err := strconv.Atoi(string(msg[0]))
	if err != nil {
		return reply.NewStanderErrReply("Err invalid DB index")
	}
	if dbIndex >= len(db.dbs) {
		return reply.NewStanderErrReply("Err DBIndex is out of range")
	}
	respConn.SelectDB(dbIndex)
	return reply.NewOKReply()
}
