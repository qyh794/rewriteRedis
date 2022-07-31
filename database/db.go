package database

import (
	"fmt"
	"rewriteRedis/datastruct/dict"
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
	"strings"
)

type DB struct {
	index int       // 分数据库的编号
	data  dict.Dict // 分数据库
}

// 构造函数
func NewDB() *DB {
	return &DB{
		data: dict.NewSyncMap(),
	}
}

// Exec 执行函数 msg -> set key value
func (d *DB) Exec(msg [][]byte) resp.Reply {
	// 获取指令类型
	cmdName := strings.ToUpper(string(msg[0]))
	// 根据类型获取对应的command
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.NewStanderErrReply(fmt.Sprintf("Err unknown command %s", cmdName))
	}
	// 判断参数个数是否合法
	if !validateArity(cmd.arity, msg) {
		return reply.NewArgNumErrReply(cmdName)
	}
	// 执行函数, set key val在比较参数是否合法时还是原来的指令,当执行对应执行函数时就只剩下 key val了
	return cmd.executor(d, msg[1:])
}

func validateArity(arity int, msg [][]byte) bool {
	msgNum := len(msg)
	// 执行方法中合法的参数个数,如果是正数,说明是定长的
	if arity >= 0 {
		return arity == msgNum
	}
	// 如果是负数,说明是不定长的
	return msgNum >= -arity
}

/* ---- 指令执行函数调用的db的方法 ----- */

// SetEntity set
func (d *DB) SetEntity(key string, value interface{}) int {
	return d.data.Set(key, value)
}

// GetEntity get
func (d DB) GetEntity(key string) (interface{}, bool) {
	res, exists := d.data.Get(key)
	if !exists {
		return nil, false
	}
	return res, true
}

// SetIfAbsent setNX
func (d *DB) SetIfAbsent(key string, value interface{}) int {
	return d.data.SetIfAbsent(key, value)
}

// Remove 移除单个key
func (d *DB) Remove(key string) {
	d.data.Remove(key)
}

func (d *DB) KeysAll() []string {
	return d.data.Keys()
}

// RemoveKeys 删除多个key,并返回删除的数量
func (d *DB) RemoveKeys(keys ...string) int {
	deleted := 0
	for i := range keys {
		_, exists := d.data.Get(keys[i])
		if exists {
			d.data.Remove(keys[i])
			deleted++
		}
	}
	return deleted
}

func (d *DB) Flush() {
	d.data.Clear()
}
