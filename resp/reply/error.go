package reply

type ErrorReply interface {
	ToByte() []byte
	Error() string
}

/* ---- UnKnow Error Reply ---- */
type unKnowErrReply struct {
}

var unKnowErrReplyObj = new(unKnowErrReply)

func NewUnKnowErrReply() *unKnowErrReply {
	return unKnowErrReplyObj
}

var unKnowErrReplyByte = []byte("-Err unKnown\r\n")

func (u *unKnowErrReply) ToByte() []byte {
	return unKnowErrReplyByte
}

func (u *unKnowErrReply) Error() string {
	return "Err unKnown"
}

/* ---- Arg Num Error Reply ---- */
type argNumErrReply struct {
	cmd string
}

func (a *argNumErrReply) ToByte() []byte {
	return []byte("-ERR wrong number of argument for '" + a.cmd + "'command\r\n")
}

func (a *argNumErrReply) Error() string {
	//TODO implement me
	panic("implement me")
}

func NewArgNumErrReply(cmd string) *argNumErrReply {
	return &argNumErrReply{
		cmd: cmd,
	}
}

/* ---- syntax Error Reply ---- */
type syntaxErrReply struct {
}

var syntaxErrReplyObj = new(syntaxErrReply)
var syntaxErrReplyByte = []byte("-Err syntax error\r\n")

func NewSyntaxErrReply() *syntaxErrReply {
	return syntaxErrReplyObj
}

func (s syntaxErrReply) ToByte() []byte {
	return syntaxErrReplyByte
}

func (s syntaxErrReply) Error() string {
	return "Err syntax error"
}

/* ---- Wrong Type Error Reply ---- */
type wrongTypeErrReply struct {
}

func (w *wrongTypeErrReply) ToByte() []byte {
	return []byte("Wrong Type Operation against a key holding the wrong kind of value\r\n")
}

func (w *wrongTypeErrReply) Error() string {
	return "Wrong Type Operation against a key holding the wrong kind of value"
}

/* ---- Protocol Error Reply ---- */
type protocolErrReply struct {
}

func (p protocolErrReply) ToByte() []byte {
	return []byte("protocol Error")
}

func (p protocolErrReply) Error() string {
	return "protocol Error"
}
