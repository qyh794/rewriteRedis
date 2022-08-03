package aof

import (
	"io"
	"os"
	"rewriteRedis/config"
	databaseface "rewriteRedis/interface/database"
	"rewriteRedis/lib/logger"
	"rewriteRedis/lib/utils"
	"rewriteRedis/resp/connection"
	"rewriteRedis/resp/parser"
	"rewriteRedis/resp/reply"
	"strconv"
)

const aofChanSize = 1 << 16

type payload struct {
	msg     [][]byte
	dbIndex int
}

type AofHandler struct {
	database    databaseface.Database
	aofFile     *os.File
	aofFilename string
	aofChan     chan *payload
	curDBIndex  int
}

// NewAofHandler aof处理器构造函数
func NewAofHandler(database databaseface.Database) (*AofHandler, error) {
	aHandler := &AofHandler{}
	aHandler.database = database
	aHandler.aofFilename = config.Properties.AppendFilename
	aHandler.loadAof()
	file, err := os.OpenFile(aHandler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	aHandler.aofFile = file
	aHandler.aofChan = make(chan *payload, aofChanSize)
	// 开启协程处理管道中的指令
	go func() {
		aHandler.handlerAofChan()
	}()
	return aHandler, nil
}

// AddToAofChan 把用户指令发送到管道中
func (a *AofHandler) AddToAofChan(dbIndex int, msg [][]byte) {
	// 当开启了aof功能,并且初始化了管道才能将指令发送到管道中
	if config.Properties.AppendOnly && a.aofChan != nil {
		a.aofChan <- &payload{
			msg:     msg,
			dbIndex: dbIndex,
		}
	}
}

// handleAof 监听管道中的数据,并把管道中的数据写入文件
func (a *AofHandler) handlerAofChan() {
	// 初始化DBIndex
	a.curDBIndex = 0
	for p := range a.aofChan {
		if p.dbIndex != a.curDBIndex {
			// 指令不是在当前db执行的,需要记录一个select指令到AOF中,将指令转为二维切片
			data := reply.NewMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToByte()
			// 将数据写入文件
			_, err := a.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			// 变更了数据库记录下这条指令选择的数据库
			a.curDBIndex = p.dbIndex
		}
		// 指令和上条指令执行在同一个DB中
		data := reply.NewMultiBulkReply(p.msg).ToByte()
		// 将数据写入文件
		_, err := a.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			continue
		}
	}
}

// loadAof 将文件中的指令重新执行一遍
func (a AofHandler) loadAof() {
	// 打开文件
	file, err := os.Open(a.aofFilename)
	if err != nil {
		logger.Warn(err)
		return
	}
	// 延迟关闭
	defer file.Close()
	// 数据库执行指令需要一个连接
	fakeConn := &connection.RespConnection{}
	// 将文件中的数据交给resp解析
	ch := parser.ParseStream(file)
	// 处理解析器返回的数据
	for p := range ch {
		if p.Err != nil {
			// 程序退出
			if p.Err == io.EOF {
				break
			}
			// 其他错误继续处理下条指令
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		// 数据为空
		if p.Data == nil {
			logger.Info("empty payload")
			continue
		}
		// 数据不为空,将数据转为二维字节切片
		bulkReply, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("required multi bulk reply")
			continue
		}
		// 数据转换没问题,交由database执行
		res := a.database.Exec(fakeConn, bulkReply.Msg)
		if reply.IsErrReply(res) {
			logger.Error("exec err: ", err)
		}
	}
}
