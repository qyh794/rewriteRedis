package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFunc func(data []byte) uint32

// NodeMap 一致性哈希管理器
type NodeMap struct {
	hashFunc    HashFunc       // 哈希函数,根据key判断节点
	nodeHash    []int          // 哈希环,保存各个节点的哈希值
	nodeHashMap map[int]string // 节点哈希值到物理节点的映射
	replicas    int            // 虚拟节点
}

// NewNodeMap 一致性哈希管理器构造方法
func NewNodeMap(hashFunc HashFunc, replicas int) *NodeMap {
	m := &NodeMap{
		hashFunc:    hashFunc,
		nodeHashMap: make(map[int]string),
		replicas:    replicas,
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// AddNode 添加节点,把集群中配置的节点哈希值都添加到哈希环中,并添加哈希值到节点的映射
func (m *NodeMap) AddNode(keys ...string) {
	for i, _ := range keys {
		if keys[i] == "" {
			continue
		}
		for j := 0; j < m.replicas; j++ {
			// 哈希函数计算出来的哈希值类型为unit32,需要把哈希值放入哈希环中,哈希环中元素为int类型
			hash := int(m.hashFunc([]byte(strconv.Itoa(j) + keys[i])))
			m.nodeHash = append(m.nodeHash, hash)
			m.nodeHashMap[hash] = keys[i]
		}
	}
	sort.Ints(m.nodeHash)
}

func (m *NodeMap) isEmpty() bool {
	return len(m.nodeHash) == 0
}

// PickNode 根据key选择节点
func (m *NodeMap) PickNode(key string) string {
	if m.isEmpty() {
		return ""
	}
	// 计算key的哈希值
	hash := int(m.hashFunc([]byte(key)))
	// 使用二分查找法搜索 keys 中满足 m.keys[i] >= hash 的最小 i 值
	index := sort.Search(len(m.nodeHash), func(i int) bool {
		return m.nodeHash[i] >= hash
	})
	// 哈希值大于环中所有节点,那么归属于第一个节点
	if index == len(m.nodeHash) {
		index = 0
	}
	// index只是下标,下标对应的元素才是物理节点对应的哈希值
	return m.nodeHashMap[m.nodeHash[index]]
}
