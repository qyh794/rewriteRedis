package main

import (
	"os"
	"rewriteRedis/config"
	"rewriteRedis/lib/logger"
	"rewriteRedis/tcp"
	"fmt"
)

// 配置文件名称
var confFile string = "redis.conf"

// 默认配置文件
var defaultProperties = &config.ServerProperties {
	Bind: "0.0.0.0", 
	Port: 8888,
}

func fileExists(filename string) bool {
	fi, err := os.Stat(filename)
	return err == nil && fi.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path: "logs",
		Name: "rewriteRedis",
		Ext: "log",
		TimeFormat: "2006-01-02",
	})
	if fileExists(confFile) {
		config.SetupConfig(confFile)
	} else {
		config.Properties = defaultProperties
	}
	err := tcp.ListenAndServerWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, tcp.NewEchoRedisHandler())
	if err != nil {
		logger.Info("something wrong with main")
		logger.Error(err)
	}
}