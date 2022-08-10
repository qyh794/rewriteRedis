package utils

func ToCmdLine(msg ...string) [][]byte {
	res := make([][]byte, len(msg))
	for i, s := range msg {
		res[i] = []byte(s)
	}
	return res
}

func ToCmdLine2(name string, msg ...[]byte) [][]byte {
	res := make([][]byte, len(msg)+1)
	res[0] = []byte(name)
	for i, bytes := range msg {
		res[i+1] = bytes
	}
	return res
}
