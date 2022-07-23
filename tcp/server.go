package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"rewriteRedis/interface/tcp"
	"rewriteRedis/lib/logger"
	"sync"
	"syscall"
)

// 启动tcp的配置
type Config struct {
	Address string
}

func ListenAndServerWithSignal(conf *Config, handler tcp.Handler) error {
	signChan := make(chan os.Signal)
	closeChan := make(chan struct{})
	signal.Notify(signChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sign := <-signChan
		switch sign{
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 监听,监听状态的socket
	listener, err := net.Listen("tcp", conf.Address)
	if err != nil {
		logger.Error(fmt.Sprintf("listen at %s failed, err:", conf.Address))
		return err
	}
	logger.Info(fmt.Sprintf("start listen at %s", conf.Address))
	ListenAndServer(listener, handler, closeChan)
	return nil
}

func ListenAndServer(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	go func() {
		<-closeChan
		logger.Info("user close program window, listener and handler will close...")
		listener.Close()
		handler.Close()
	}()
	defer listener.Close()
	defer handler.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()	// 建立连接
		if err != nil {
			logger.Info("something wrong with listener.Accept(), err: ", err)
			break
		}
		// 接收新连接可能会出错,防止已建立的连接未处理完毕程序直接退出
		wg.Add(1)
		go func ()  {
			defer wg.Done()
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}