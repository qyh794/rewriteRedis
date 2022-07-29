package reply

/* ---- Pong Reply ---- */
type pongReply struct {
}

var pongReplyObj = new(pongReply)

func NewPongReply() *pongReply {
	return pongReplyObj
}

var pongReplyByte = []byte("+PONG\r\n")

func (p *pongReply) ToByte() []byte {
	return pongReplyByte
}

/* ---- OK Reply ---- */
type okReply struct {
}

var okReplyObj = new(okReply)

func NewOKReply() *okReply {
	return okReplyObj
}

var okReplyByte = []byte("+OK\r\n")

func (o *okReply) ToByte() []byte {
	return okReplyByte
}

/* ---- Null Bulk Reply ---- */
type nullBulkReply struct {
}

var nullBulkReplyObj = new(nullBulkReply)

func NewNullBulkReply() *nullBulkReply {
	return nullBulkReplyObj
}

var nullBulkReplyByte = []byte("$-1\r\n")

func (n *nullBulkReply) ToByte() []byte {
	return nullBulkReplyByte
}

/* ---- Empty Multi Bulk Reply ---- */
type emptyMultiBulkReply struct {
}

var emptyMultiBulkReplyObj = new(emptyMultiBulkReply)

func NewEmptyMultiBulkReply() *emptyMultiBulkReply {
	return emptyMultiBulkReplyObj
}

var emptyMultiBulkReplyByte = []byte("*0\r\n")

func (e *emptyMultiBulkReply) ToByte() []byte {
	return emptyMultiBulkReplyByte
}

/* ---- Empty Reply ---- */
type emptyReply struct {
}

var emptyReplyObj = new(emptyReply)

func NewEmptyReply() *emptyReply {
	return emptyReplyObj
}

var emptyReplyByte = []byte("")

func (e *emptyReply) ToByte() []byte {
	return emptyReplyByte
}
