package database

import (
	"rewriteRedis/aof"
	"rewriteRedis/config"
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
	"strconv"
	"strings"
)

type database struct {
	dbs        []*DB
	aofHandler *aof.AofHandler
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
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(redisDB)
		if err != nil {
			panic(err)
		}
		redisDB.aofHandler = aofHandler
		// 给每一个分数据库初始化对应的addAof()函数,每一个分数据库的addAof()函数包含redis
		//数据库aofHandler成员的AddAof方法,用于在分数据库执行指令时将指令写入管道中
		for i := range redisDB.dbs {
			singleDB := redisDB.dbs[i]
			singleDB.addAof = func(msg [][]byte) {
				redisDB.aofHandler.AddToAofChan(singleDB.index, msg)
			}
		}
	}
	return redisDB
}

// 指令 set key value
func (d *database) Exec(respConn resp.Connection, msg [][]byte) resp.Reply {
	cmdName := strings.ToUpper(string(msg[0]))
	// 如果指令是select 1
	if cmdName == "SELECT" {
		if len(msg) != 2 { // 如果是select
			return reply.NewArgNumErrReply("select")
		}
		return execSelect(respConn, msg[1:], d)
	}
	// 其他指令,需要分数据库去执行
	// 1.获取该指令需要的分数据库
	dbIndex := respConn.GetDBIndex()
	if dbIndex >= len(d.dbs) {
		return reply.NewStanderErrReply("ERR db index is out of range")
	}
	db := d.dbs[dbIndex]
	return db.Exec(msg)
}

func (d *database) Close() {

}

func (d *database) AfterClientClose(respConn resp.Connection) {

}

// execSelect select 1指令传到这只剩下 1
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
