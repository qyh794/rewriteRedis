package cluster

import (
	"fmt"
	"rewriteRedis/interface/resp"
	"rewriteRedis/lib/utils"
	"rewriteRedis/resp/reply"
)

type CmdLine = [][]byte

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["PING"] = ping
	routerMap["SET"] = defaultFunc
	routerMap["GET"] = defaultFunc
	routerMap["STRLEN"] = defaultFunc
	routerMap["SETNX"] = defaultFunc
	routerMap["GETSET"] = defaultFunc
	routerMap["TYPE"] = defaultFunc
	routerMap["DEL"] = del
	routerMap["EXISTS"] = exists
	routerMap["FLUSHDB"] = flushDB
	routerMap["RENAME"] = rename
	return routerMap
}

// defaultFunc 转发
func defaultFunc(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	// 用户发送指令,根据指令的key判断要去的节点
	key := string(cmdMsg[1])
	node := cluster.peerPicker.PickNode(key)
	return cluster.relay(node, respConn, cmdMsg)
}

// ping 本地指令
func ping(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	return cluster.database.Exec(respConn, cmdMsg)
}

// del 群发指令 del k1 k2...
func del(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {

	if len(cmdMsg) < 2 {
		return reply.NewArgNumErrReply(fmt.Sprintf("wrong args num with %s", string(cmdMsg[0])))
	}

	var deleted int64 = 0
	for i := 1; i < len(cmdMsg); i++ {
		node := cluster.peerPicker.PickNode(string(cmdMsg[i]))
		relay := cluster.relay(node, respConn, utils.ToCmdLine2("DEL", cmdMsg[i]))
		count, ok := relay.(*reply.IntReply)
		if !ok {
			continue
		}
		deleted += count.Code
	}

	return reply.NewIntReply(deleted)
}

// exists 群发指令 exists k1
func exists(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	if len(cmdMsg) < 2 {
		return reply.NewArgNumErrReply(fmt.Sprintf("%s", string(cmdMsg[0])))
	}
	var existed int64 = 0
	for i := 1; i < len(cmdMsg); i++ {
		key := string(cmdMsg[i])
		node := cluster.peerPicker.PickNode(key)

		res := cluster.relay(node, respConn, utils.ToCmdLine2("exists", cmdMsg[i]))
		count, ok := res.(*reply.IntReply)
		if !ok {
			continue
		}
		existed += count.Code
	}
	return reply.NewIntReply(existed)
}

// flushDB 群发指令 exists k1 k2...
func flushDB(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	var resRep resp.Reply
	var errRep reply.ErrorReply
	for i := 0; i < len(cluster.peers); i++ {
		resRep = cluster.relay(cluster.peers[i], respConn, cmdMsg)
		if reply.IsErrReply(resRep) {
			errRep = resRep.(reply.ErrorReply)
			break
		}
	}
	if errRep != nil {
		return reply.NewStanderErrReply("error occurs: " + errRep.Error())
	}
	return reply.NewOKReply()
}

// rename 群发指令 rename oldKey newKey
func rename(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
	// 如果指令长度不等于3,说明指令错误
	if len(cmdMsg) != 3 {
		return reply.NewStanderErrReply("ERR wrong number of arguments for 'rename' command")
	}

	// 查找oldKey所在节点
	srcNode := cluster.peerPicker.PickNode(string(cmdMsg[1]))
	// 查找newKey所在节点
	destNode := cluster.peerPicker.PickNode(string(cmdMsg[2]))

	/*<--------rename前后key落在同一个节点-------->*/
	if srcNode == destNode {
		return cluster.relay(srcNode, respConn, cmdMsg)
	}

	/*<--------rename前后key落在不同节点-------->*/

	// 获取oldKey原先的val
	oldVal := defaultFunc(cluster, respConn, utils.ToCmdLine2("get", cmdMsg[1])).ToByte()
	// 编辑删除指令
	delCmd := utils.ToCmdLine2("del", cmdMsg[1])
	// 遍及插入指令
	setCmd := utils.ToCmdLine2("set", cmdMsg[2], oldVal)
	// 将删除指令转发给oldKey所在节点,将oldKey删除
	relay := cluster.relay(srcNode, respConn, delCmd)
	if reply.IsErrReply(relay) {
		return reply.NewStanderErrReply("rename failed")
	}
	// 将插入指令转发给newKey所在节点,插入newKey
	relay = cluster.relay(destNode, respConn, setCmd)
	if reply.IsErrReply(relay) {
		return reply.NewStanderErrReply("rename failed")
	}
	// 成功返回OK
	return reply.NewOKReply()
}

// keys 群发指令 keys *
//func keys(cluster *ClusterDatabase, respConn resp.Connection, cmdMsg [][]byte) resp.Reply {
//	resMap := make(map[string]*reply.MultiBulkReply)
//	for i := 0; i < len(cluster.peers); i++ {
//		relay := cluster.relay(cluster.peers[i], respConn, cmdMsg) // MultiBulkReply {msg [][]byte}
//		bulkReply, ok := relay.(*reply.MultiBulkReply)
//		if bulkReply == nil {
//			continue
//		}
//		if !ok {
//			return reply.NewStanderErrReply("something wrong with cluster keys()")
//		}
//		resMap[cluster.peers[i]] = bulkReply
//	}
//	var res [][]byte
//	for _, v := range resMap {
//		if res == nil {
//			res = v.Msg
//			continue
//		}
//		for i := 0; i < len(v.Msg); i++ {
//			res = append(res, v.Msg[i])
//		}
//	}
//	return reply.NewMultiBulkReply(res)
//}
