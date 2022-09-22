package parser

import (
	"bufio"
	"errors"
	"io"
	"rewriteRedis/interface/resp"
	"rewriteRedis/resp/reply"
	"strconv"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

// parserState 解析器状态
type parserState struct {
	readingMultiLine  bool     // 读取单行还是多行
	expectedArgsCount int      // 读取的指令应该有的参数个数,例如set key val一共三个参数
	msgType           byte     // 数据类型,数组还是单行字符串
	msg               [][]byte // 指令, set key val-> [[set],[key],[val]]
	bulkLen           int64    // len(msg[i])
}

// finish 解析指令是否完成
func (p *parserState) finish() bool {
	return p.expectedArgsCount > 0 && p.expectedArgsCount == len(p.msg)
}

// ParseStream  协议层对外接口 把redis业务处理和协议解析并发进行，解析字节流会返回一个管道，把解析的结果从管道中返回
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

func parse(reader io.Reader, ch chan<- *Payload) {
	// 读取用户指令
	br := bufio.NewReader(reader)
	var state parserState
	var curMsg []byte
	var err error
	// 循环解析用户指令
	for {
		var ioErr bool
		curMsg, ioErr, err = readLine(br, &state)
		if err != nil {
			if ioErr { // io错误
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// 协议错误
			ch <- &Payload{
				Err: err,
			}
			// 重置解析器状态 
			state = parserState{}
			continue
		}
		// 单行模式,第一次读取指令
		if !state.readingMultiLine {
			if curMsg[0] == '*' {
				err = parseMultiBulkHeader(curMsg, &state)

				//err = parseMultiBulkHeader1(curMsg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = parserState{}
					continue
				}
				// *0\r\n, 表示后面没有数据,不用再解析了
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: reply.NewEmptyMultiBulkReply(),
					}
					state = parserState{}
					continue
				}
			} else if curMsg[0] == '$' { // 第一次读取开头就是$3\r\n
				err = parseBulkHeader(curMsg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = parserState{}
					continue
				}
				if state.bulkLen == -1 { // $-1\r\n -> []
					ch <- &Payload{
						Data: reply.NewNullBulkReply(),
					}
					state = parserState{}
					continue
				}
			} else { // 简单指令, +ok\r\n
				line, err := parseSingleLine(curMsg)
				ch <- &Payload{
					Data: line,
					Err:  err,
				}
				state = parserState{}
				continue
			}
		} else { // 多行模式, $3\r\n 或者 set\r\n
			err = readBody(curMsg, &state)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				state = parserState{}
				continue
			}
			// 解析结束
			if state.finish() {
				var res resp.Reply
				if state.msgType == '*' {
					res = reply.NewMultiBulkReply(state.msg)
				} else if state.msgType == '$' { // $4\r\nping\r\n -> [[ping]]
					res = reply.NewBulkReply(state.msg[0])
				}
				ch <- &Payload{
					Data: res,
					Err:  err,
				}
				state = parserState{}
			}
		}
	}
}

// readLine 以\r\n读取一行用户指令, 返回的是 *3\r\n 或者 $3\r\n
func readLine(br *bufio.Reader, state *parserState) ([]byte, bool, error) {
	//msg := make([]byte, state.bulkLen)
	var curMsg []byte
	var err error
	if state.bulkLen == 0 { // 1.如果没有读到$,字节组数据块的长度等于0,按照\r\n
		curMsg, err = br.ReadBytes('\n')
		if err != nil { // io错误
			return nil, true, err
		}
		if len(curMsg) == 0 || curMsg[len(curMsg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(curMsg))
		}
	} else { //2.如果读到了$,那么只能连续读取$后面数字个字符 + \r\n 才能结束
		curMsg = make([]byte, state.bulkLen+2)
		//把 br里的数据读到msg中
		_, err = io.ReadFull(br, curMsg)
		if err != nil {
			return nil, true, err
		}
		if len(curMsg) == 0 || curMsg[len(curMsg)-1] != '\n' || curMsg[len(curMsg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(curMsg))
		}
		state.bulkLen = 0
	}
	return curMsg, false, nil
}

// parseMultiBulkHeader 第一次读取用户指令,应当设置解析器的状态,处理以*3\r\n开头的指令
func parseMultiBulkHeader(curMsg []byte, state *parserState) error {
	// expected读取的指令应该有的参数个数,也是[][]msg的长度
	var expected uint64
	expected, err := strconv.ParseUint(string(curMsg[1:len(curMsg)-2]), 10, 32)
	//expected, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(curMsg))
	}
	if expected == 0 { // *0\r\n、$0\r\n
		state.expectedArgsCount = 0
		return nil
	} else if expected > 0 { // $-1\r\n
		// 变为多行模式
		state.readingMultiLine = true
		// 设置参数个数
		state.expectedArgsCount = int(expected)
		// 设置参数类型
		state.msgType = curMsg[0]
		// 初始化[][]msg
		state.msg = make([][]byte, 0, expected)
		return nil
	} else {
		return errors.New("protocol error: " + string(curMsg))
	}
}

// parseBulkHeader 第一次读取用户指令,解析单行指令, 例如 以$4\r\n开头
func parseBulkHeader(curMsg []byte, state *parserState) error {
	var err error
	// $4\r\n, bulkLen = 4
	state.bulkLen, err = strconv.ParseInt(string(curMsg[1:len(curMsg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(curMsg))
	}
	
	if state.bulkLen > 0 { // ping\r\n -> [[ping]]
		state.readingMultiLine = true
		state.msgType = curMsg[0]
		state.expectedArgsCount = 1
		state.msg = make([][]byte, 0, 1)
		return nil
	} else if state.bulkLen == -1 {
		return nil
	} else {
		return errors.New("protocol error: " + string(curMsg))
	}
}

// parseSingleLine 解析单行指令 +ok\r\n、-err\r\n,碰到这种直接按照类型返回
func parseSingleLine(curMsg []byte) (resp.Reply, error) {
	str := string(curMsg[1 : len(curMsg)-2])
	var res resp.Reply
	switch curMsg[0] {
	case '+':
		res = reply.NewStatusReply(str)
	case '-':
		res = reply.NewStanderErrReply(str)
	case ':':
		code, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(curMsg))
		}
		res = reply.NewIntReply(code)
	}
	return res, nil
}

// readBody 非第一次读取用户指令,例如读取到第二行$3\r\n或者set\r\n
func readBody(curMsg []byte, state *parserState) error {
	line := curMsg[:len(curMsg)-2]
	var err error
	if line[0] == '$' { // $3\r\n
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(curMsg))
		}
		if state.bulkLen <= 0 { //$0\r\n, 往[][]msg中加入空切片即可
			state.msg = append(state.msg, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.msg = append(state.msg, line)
	}
	return nil
}
