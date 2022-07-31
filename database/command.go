package database

import (
	"rewriteRedis/interface/resp"
	"strings"
)

var cmdTable = make(map[string]*command) // 存储所有的指令,以及指令对应的执行方法

type ExecFunc func(db *DB, msg [][]byte) resp.Reply

type command struct {
	executor ExecFunc
	arity    int // 执行方法应该有的参数个数
}

// RegisterCommand  注册指令的方法,把指令注册到cmdTable中,db就可以直接通过指令名称获取到对应的执行方法
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToUpper(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
