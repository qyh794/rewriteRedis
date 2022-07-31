package dict

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Set(key string, val interface{}) int
	Len() int
	SetIfAbsent(key string, val interface{}) int
	Remove(key string) int
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
