package resp

type Reply interface {
	ToByte() []byte
}
