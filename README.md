# rewriteRedis
rewriteRedis by Golang


参考开源项目https://github.com/HDT3213/godis

实现了redis的部分功能,8大数据类型目前只实现了string,如果想实现更完整的go版本redis请前往大佬写的开源项目https://github.com/HDT3213/godis


单机版已经实现指令：

  set key val
  
  get key
  
  setnx key val
  
  getset key val
  
  strlen key
  
  del key1, key2...
  
  exists key1, key2...
  
  flushdb
  
  type key
  
  rename key1 key2
  
  renamenx key1 key2
  
  keys *
  
  ping
  
  select 1
  
集群版已经实现指令：

  ping
  
  set key val
  
  get key
  
  strlen key
  
  setnx key val
  
  getset key val
  
  type key
  
  del key1, key2...
  
  exists key1, key2...
  
  flushdb
  
  rename key1 key2
  
  renamenx key1 key2
  
  select 1
  
使用tcp连接工具例如net assist连接项目服务端时,需要发送resp协议格式的指令,例如:

  *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n
  
  *2\r\n$3\r\nget\r\n$3\r\nkey\r\n
  

如果需要开启集群模式,需要在配置文件redis.conf中添加self和peers,分别对应自己的地址和集群中其他兄弟节点的地址,多个兄弟节点之间用','隔开
