package dict

import "sync"

type syncMap struct {
	m sync.Map
}

func NewSyncMap() *syncMap {
	return &syncMap{}
}

func (s *syncMap) Get(key string) (val interface{}, exists bool) {
	return s.m.Load(key)
}

func (s *syncMap) Set(key string, val interface{}) int {
	count := 0
	_, ok := s.m.Load(key)
	if !ok {
		count++
	}
	s.m.Store(key, val)
	return count
}

func (s *syncMap) Len() int {
	count := 0
	s.m.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// SetIfAbsent 不存在就添加 setNX
func (s *syncMap) SetIfAbsent(key string, val interface{}) int {
	_, ok := s.m.Load(key)
	if ok { // 存在
		return 0
	}
	s.m.Store(key, val)
	return 1
}

// Remove del key
func (s *syncMap) Remove(key string) int {
	_, ok := s.m.Load(key)
	s.m.Delete(key)
	if ok {
		return 1
	}
	return 0
}

// Keys keys*
func (s *syncMap) Keys() []string {
	res := make([]string, s.Len())
	index := 0
	s.m.Range(func(key, value any) bool {
		res[index] = key.(string)
		index++
		return true
	})
	return res
}

//
func (s *syncMap) RandomKeys(limit int) []string {
	res := make([]string, limit)
	for i := 0; i < limit; i++ {
		s.m.Range(func(key, value any) bool {
			res[i] = key.(string)
			return false
		})
	}
	return res
}

func (s *syncMap) RandomDistinctKeys(limit int) []string {
	if limit == 0 {
		return []string{}
	}
	res := make([]string, limit)
	index := 0
	s.m.Range(func(key, value any) bool {
		res[index] = key.(string)
		index++
		if index == limit {
			return false
		}
		return true
	})
	return res
}

func (s *syncMap) Clear() {
	s = NewSyncMap()
}

