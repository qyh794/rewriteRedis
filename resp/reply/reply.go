package reply

import (
	"rewriteRedis/interface/resp"
	"strconv"
)

var (
	CRLF                = "\r\n"
	nullBulkReplyAppend = []byte("$-1")
)

/* ---- Bulk Reply ---- */
type bulkReply struct {
	Msg []byte // "abc" -> "$3\r\nabc\r\n"
}

func (b *bulkReply) ToByte() []byte {
	return []byte("$" + strconv.Itoa(len(b.Msg)) + CRLF + string(b.Msg) + CRLF)
}

func NewBulkReply(msg []byte) *bulkReply {
	return &bulkReply{
		Msg: msg,
	}
}

/* ---- Multi Bulk Reply ---- */
type MultiBulkReply struct {
	Msg [][]byte // "Set Key Val" -> "*3\r\n$3\r\nSet\r\n$3\r\nKey\r\n$3\r\nVal\r\n"
}

func (m *MultiBulkReply) ToByte() []byte {
	res := ""
	res = res + "*" + strconv.Itoa(len(m.Msg)) + CRLF
	for i := 0; i < len(m.Msg); i++ {
		if m.Msg[i] == nil {
			res += string(nullBulkReplyAppend) + CRLF
		} else {
			res += "$" + strconv.Itoa(len(m.Msg[i])) + CRLF + string(m.Msg[i]) + CRLF
		}
	}
	return []byte(res)
}

func NewMultiBulkReply(msg [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Msg: msg,
	}
}

/* ---- Status Reply ---- */
type statusReply struct {
	Status string // "wow" -> "+wow\r\n"
}

func (s *statusReply) ToByte() []byte {
	return []byte("+" + s.Status + CRLF)
}

func NewStatusReply(status string) *statusReply {
	return &statusReply{Status: status}
}

/* ---- Standard Error Reply ---- */
type standardErrReply struct {
	Status string //"status" -> "-status\r\n"
}

func (s *standardErrReply) ToByte() []byte {
	return []byte("-" + s.Status + CRLF)
}

func (s *standardErrReply) Error() string {
	return s.Status
}

func NewStanderErrReply(status string) *standardErrReply {
	return &standardErrReply{Status: status}
}

/* ---- Int Reply  ---- */
type intReply struct {
	Code int64 //"1" -> ":1\r\n"
}

func NewIntReply(code int64) *intReply {
	return &intReply{Code: code}
}

func (i *intReply) ToByte() []byte {
	return []byte("+" + strconv.Itoa(int(i.Code)) + CRLF)
}

/* ---- Is Error Reply ---- */

// IsErrReply 判断第一个字节可以知道是正常回复还是异常回复
func IsErrReply(reply resp.Reply) bool {
	return reply.ToByte()[0] == '-'
}
