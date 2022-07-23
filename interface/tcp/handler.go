package tcp

import (
	"context"
	"net"
)

// 业务逻辑处理,当tcp层接收新连接后交由Handler处理这个连接
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}