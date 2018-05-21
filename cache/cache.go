package cache

import (
	"io"
)

// ErrTooBig is returned by Cache::Put when when the item size is bigger than the
// cache size limit.
type ErrTooBig struct{}

func (e *ErrTooBig) Error() string {
	return "item bigger than the cache size limit"
}

// Cache is the interface for a generic blob storage backend. Implementers should handle
// locking internally.
type Cache interface {
	// Put stores a stream of `size` bytes from `r` into the cache. If `expectedSha256` is
	// not the empty string, and the contents don't match it, an error is returned
	Put(key string, size int64, expectedSha256 string, r io.Reader) error
	// Get writes the content of the cache item stored under `key` to `w`. If the item is
	// not found, it returns ok = false.
	Get(key string, fromActionCache bool) (data io.ReadCloser, sizeBytes int64, err error)
	Contains(key string, fromActionCache bool) (ok bool)

	// Stats
	MaxSize() int64
	CurrentSize() int64
	NumItems() int
}
