package dict

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.Mutex
}

// NewConcurrent 初始化
func NewConcurrent(shardCount int) *ConcurrentDict {
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	cur := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return cur
}

const prime32 = uint32(16777619)

//fnv32 哈希函数计算key哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

//spread 根据key哈希值计算位于哪个分段上
func (c *ConcurrentDict) spread(hash uint32) uint32 {
	if c == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(c.table))
	return (tableSize - 1) & uint32(hash)
}

//getShard 获取分段
func (c *ConcurrentDict) getShard(index uint32) *shard {
	if c == nil {
		panic("dict is nil")
	}
	return c.table[index]
}

func (c *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	// 判断是否初始化
	if c == nil {
		panic("dict is nil")
	}
	// 计算key哈希值
	hash := fnv32(key)
	// 获取key所处分段
	index := c.spread(hash)
	// 获取计算出来的分段
	curShard := c.getShard(index)
	// 加锁
	curShard.mutex.Lock()
	// 解锁
	defer curShard.mutex.Unlock()
	// 查询并返回
	val, exists = curShard.m[key]
	return
}

func (c *ConcurrentDict) Set(key string, val interface{}) int {
	// 判断是否初始化
	if c == nil {
		panic("dict is nil")
	}
	// 计算key哈希值
	hash := fnv32(key)
	// 获取key所处分段
	index := c.spread(hash)
	// 获取计算出来的分段
	curShard := c.getShard(index)
	// 加锁
	curShard.mutex.Lock()
	// 解锁
	defer curShard.mutex.Unlock()
	// 如果是修改返回0
	if _, exists := curShard.m[key]; exists {
		curShard.m[key] = val
		return 0
	}
	// 添加key,返回1
	curShard.m[key] = val
	c.addCount()
	return 1
}

func (c *ConcurrentDict) Len() int {
	if c == nil {
		panic("dict is nil")
	}
	// 返回count
	return int(c.count)
}

// SetIfAbsent 不存在则添加
func (c *ConcurrentDict) SetIfAbsent(key string, val interface{}) int {
	// 判断是否初始化
	if c == nil {
		panic("dict is nil")
	}
	// 计算key哈希值
	hash := fnv32(key)
	// 获取key所处分段
	index := c.spread(hash)
	// 获取计算出来的分段
	curShard := c.getShard(index)
	// 加锁
	curShard.mutex.Lock()
	// 解锁
	defer curShard.mutex.Unlock()
	// 如果key存在则返回0
	if _, exists := curShard.m[key]; exists {
		return 0
	}
	// 如果key不存在添加key,返回
	curShard.m[key] = val
	c.addCount()
	return 1
}

func (c *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&c.count, 1)
}

func (c *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&c.count, -1)
}

func (c *ConcurrentDict) Remove(key string) int {
	// 判断是否初始化
	if c == nil {
		panic("dict is nil")
	}
	// 计算key哈希值
	hash := fnv32(key)
	// 获取key所处分段
	index := c.spread(hash)
	// 获取计算出来的分段
	curShard := c.getShard(index)
	// 加锁
	curShard.mutex.Lock()
	// 解锁
	defer curShard.mutex.Unlock()
	// 如果key存在则删除
	if _, exists := curShard.m[key]; exists {
		delete(curShard.m, key)
		c.decreaseCount()
		return 1
	}
	// 如果key不存在,返回
	return 0
}

func (c *ConcurrentDict) ForEach(consumer Consumer) {
	if c == nil {
		panic("dict is nil")
	}
	// 遍历每个分段
	for _, s := range c.table {
		s.mutex.Lock()
		// 这里需要使用一个匿名函数,不然在for循环中调用defer可能会存在资源泄露
		func() {
			defer s.mutex.Unlock()
			for key, val := range s.m {
				continues := consumer(key, val)
				if !continues {
					return
				}
			}
		}()
	}
}

func (c *ConcurrentDict) Keys() []string {
	res := make([]string, c.Len())
	index := 0
	c.ForEach(func(key string, val interface{}) bool {
		if index < len(res) {
			res[index] = key
			index++
		} else {
			res = append(res, key)
		}
		return true
	})
	return res
}

func (s *shard) RandomKey() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for key := range s.m {
		return key
	}
	return ""
}

func (c *ConcurrentDict) RandomKeys(limit int) []string {
	size := c.Len()
	if limit >= size {
		return c.Keys()
	}
	// 获取分段数量
	shardNum := len(c.table)
	res := make([]string, limit)
	// 遍历需要key的数量
	for i := 0; i < limit; {
		// 生成范围在分段数量之内的随机数
		// 根据随机数获取指定分段
		curShard := c.getShard(uint32(rand.Intn(shardNum)))
		// 从分段中获取数据
		if curShard == nil {
			continue
		}
		key := curShard.RandomKey()
		if key != "" {
			res[i] = key
			i++
		}
	}
	return res
}

func (c *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := c.Len()
	if limit >= size {
		return c.Keys()
	}
	// 获取分段数量
	shardNum := len(c.table)
	// temp 用于记录遍历到的key
	temp := make(map[string]struct{})
	// 循环 temp大小 小于 limit就一直遍历
	for len(temp) < limit {
		// 生成一个范围在分片数量内的随机数,这个随机数就是抽到的分段
		// 获取分段
		curShard := c.getShard(uint32(rand.Intn(shardNum)))
		// 从分段中获取key
		key := curShard.RandomKey()
		if key == "" {
			continue
		}
		// 如果key存在于temp中,跳过进行下次循环
		if _, exists := temp[key]; exists {
			continue
		}
		// temp中不存在key,加入temp中
		temp[key] = struct{}{}
	}
	res := make([]string, limit)
	index := 0
	// 遍历temp,获取所有的key
	for key, _ := range temp {
		res[index] = key
		index++
	}
	return res
}

func (c *ConcurrentDict) Clear() {
	// 重新给dict赋值一个新值即可
	*c = *NewConcurrent(c.shardCount)
}
