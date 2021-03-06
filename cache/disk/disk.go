package disk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/buchgr/bazel-remote/cache"
	"github.com/djherbis/atime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_hits",
		Help: "The total number of disk backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_misses",
		Help: "The total number of disk backend cache misses",
	})
)

// lruItem is the type of the values stored in SizedLRU to keep track of items.
// It implements the SizedItem interface.
type lruItem struct {
	size      int64
	committed bool
}

func (i *lruItem) Size() int64 {
	return i.size
}

// DiskCache is filesystem-based cache, with an optional backend proxy.
type DiskCache struct {
	logger cache.Logger
	dir    string
	proxy  cache.CacheProxy

	mu  *sync.Mutex
	lru SizedLRU
}

type nameAndInfo struct {
	name string // relative path
	info os.FileInfo
}

const sha256HashStrSize = sha256.Size * 2 // Two hex characters per byte.

// New returns a new instance of a filesystem-based cache rooted at `dir`,
// with a maximum size of `maxSizeBytes` bytes and an optional backend `proxy`.
// DiskCache is safe for concurrent use.
func New(logger cache.Logger, dir string, maxSizeBytes int64, proxy cache.CacheProxy) (*DiskCache, error) {
	// Create the directory structure.
	hexLetters := []byte("0123456789abcdef")
	for _, c1 := range hexLetters {
		for _, c2 := range hexLetters {
			subDir := string(c1) + string(c2)
			err := os.MkdirAll(filepath.Join(dir, cache.CAS.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.AC.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.RAW.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}

	// The eviction callback deletes the file from disk.
	// This function is only called while the lock is held
	// by the current goroutine.
	onEvict := func(key Key, value SizedItem) {

		f := filepath.Join(dir, key.(string))

		if value.(*lruItem).committed {
			// Common case. Just remove the cache file and we're done.
			err := os.Remove(f)
			if err != nil {
				logger.Printf("ERROR: failed to remove evicted cache file: %s", f)
			}

			return
		}

		// There is an ongoing upload for the evicted item. The temp
		// file may or may not exist at this point.
		//
		// We should either be able to remove both the temp file and
		// the regular cache file, or to remove just the regular cache
		// file. The temp file is renamed/moved to the regular cache
		// file without holding the lock, so we must try removing the
		// temp file first.

		// Note: if you hit this case, then your cache size might be
		// too small (blobs are moved to the most-recently used end
		// of the index when the upload begins, and these items are
		// still uploading when they reach the least-recently used
		// end of the index).

		tf := f + ".tmp"
		var fErr, tfErr error
		removedCount := 0

		tfErr = os.Remove(tf)
		if tfErr == nil {
			removedCount++
		}

		fErr = os.Remove(f)
		if fErr == nil {
			removedCount++
		}

		// We expect to have removed at least one file at this point.
		if removedCount == 0 {
			if !os.IsNotExist(tfErr) {
				logger.Printf("ERROR: failed to remove evicted item: %s / %v",
					tf, tfErr)
			}

			if !os.IsNotExist(fErr) {
				logger.Printf("ERROR: failed to remove evicted item: %s / %v",
					f, fErr)
			}
		}
	}

	c := &DiskCache{
		logger: logger,
		dir:    filepath.Clean(dir),
		proxy:  proxy,
		mu:     &sync.Mutex{},
		lru:    NewSizedLRU(maxSizeBytes, onEvict),
	}

	err := c.migrateDirectories()
	if err != nil {
		return nil, fmt.Errorf("Attempting to migrate the old directory structure to the new structure failed "+
			"with error: %v", err)
	}
	err = c.loadExistingFiles()
	if err != nil {
		return nil, fmt.Errorf("Loading of existing cache entries failed due to error: %v", err)
	}

	return c, nil
}

func (c *DiskCache) migrateDirectories() error {
	err := c.migrateDirectory(filepath.Join(c.dir, cache.AC.String()))
	if err != nil {
		return err
	}
	err = c.migrateDirectory(filepath.Join(c.dir, cache.CAS.String()))
	if err != nil {
		return err
	}
	// Note: there are no old "RAW" directories (yet).
	return nil
}

func (c *DiskCache) migrateDirectory(dir string) error {
	c.logger.Printf("Migrating files (if any) to new directory structure: %s\n", dir)
	return filepath.Walk(dir, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			c.logger.Printf("Error while walking directory: %v", err)
			return err
		}

		if info.IsDir() {
			if name == dir {
				return nil
			}
			return filepath.SkipDir
		}
		hash := filepath.Base(name)
		newName := filepath.Join(filepath.Dir(name), hash[:2], hash)
		return os.Rename(name, newName)
	})
}

// loadExistingFiles lists all files in the cache directory, and adds them to the
// LRU index so that they can be served. Files are sorted by access time first,
// so that the eviction behavior is preserved across server restarts.
func (c *DiskCache) loadExistingFiles() error {
	c.logger.Printf("Loading existing files in %s.", c.dir)

	// Walk the directory tree
	var files []nameAndInfo
	err := filepath.Walk(c.dir, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			c.logger.Printf("Error while walking directory: %v", err)
			return err
		}

		if !info.IsDir() {
			files = append(files, nameAndInfo{name: name, info: info})
		}
		return nil
	})
	if err != nil {
		return err
	}

	c.logger.Printf("Sorting cache files by atime.")
	// Sort in increasing order of atime
	sort.Slice(files, func(i int, j int) bool {
		return atime.Get(files[i].info).Before(atime.Get(files[j].info))
	})

	c.logger.Printf("Building LRU index.")
	for _, f := range files {
		relPath := f.name[len(c.dir)+1:]
		ok := c.lru.Add(relPath, &lruItem{
			size:      f.info.Size(),
			committed: true,
		})
		if !ok {
			err = os.Remove(filepath.Join(c.dir, relPath))
			if err != nil {
				return err
			}
		}
	}

	c.logger.Printf("Finished loading disk cache files.")
	return nil
}

// Put stores a stream of `expectedSize` bytes from `r` into the cache.
// If `hash` is not the empty string, and the contents don't match it,
// a non-nil error is returned.
func (c *DiskCache) Put(kind cache.EntryKind, hash string, expectedSize int64, r io.Reader) error {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return fmt.Errorf("Invalid hash size: %d, expected: %d",
			len(hash), sha256.Size)
	}

	key := cacheKey(kind, hash)

	c.mu.Lock()

	// If there's an ongoing upload (i.e. cache key is present in uncommitted state),
	// we drop the upload and discard the incoming stream. We do accept uploads
	// of existing keys, as it should happen relatively rarely (e.g. race
	// condition on the bazel side) but it's useful to overwrite poisoned items.
	if existingItem, found := c.lru.Get(key); found {
		if !existingItem.(*lruItem).committed {
			c.mu.Unlock()
			io.Copy(ioutil.Discard, r)
			return nil
		}
	}

	// Try to add the item to the LRU.
	newItem := &lruItem{
		size:      expectedSize,
		committed: false,
	}
	ok := c.lru.Add(key, newItem)
	c.mu.Unlock()
	if !ok {
		return &cache.Error{
			Code: http.StatusInsufficientStorage,
			Text: "The item that has been tried to insert was too big.",
		}
	}

	// By the time this function exits, we should either mark the LRU item as committed
	// (if the upload went well), or delete it. Capturing the flag variable is not very nice,
	// but this stuff is really easy to get wrong without defer().
	shouldCommit := false
	filePath := cacheFilePath(kind, c.dir, hash)
	defer func() {
		c.mu.Lock()
		if shouldCommit {
			newItem.committed = true
		} else {
			c.lru.Remove(key)
		}
		c.mu.Unlock()

		if shouldCommit && c.proxy != nil {
			// TODO: buffer in memory, avoid a filesystem round-trip?
			fr, err := os.Open(filePath)
			if err == nil {
				c.proxy.Put(kind, hash, expectedSize, fr)
			}
		}
	}()

	// Download to a temporary file
	tmpFilePath := filePath + ".tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if !shouldCommit {
			// Only delete the temp file if moving it didn't succeed.
			os.Remove(tmpFilePath)
		}
		// Just in case we didn't already close it.  No need to check errors.
		f.Close()
	}()

	var bytesCopied int64 = 0
	if kind == cache.CAS {
		hasher := sha256.New()
		if bytesCopied, err = io.Copy(io.MultiWriter(f, hasher), r); err != nil {
			return err
		}
		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != hash {
			return fmt.Errorf(
				"hashsums don't match. Expected %s, found %s", key, actualHash)
		}
	} else {
		if bytesCopied, err = io.Copy(f, r); err != nil {
			return err
		}
	}

	if err = f.Sync(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}

	if bytesCopied != expectedSize {
		return fmt.Errorf(
			"sizes don't match. Expected %d, found %d", expectedSize, bytesCopied)
	}

	// Rename to the final path
	err = os.Rename(tmpFilePath, filePath)
	if err == nil {
		// Only commit if renaming succeeded.
		// This flag is used by the defer() block above.
		shouldCommit = true
	}

	return err
}

// Return two bools, `available` is true if the item is in the local
// cache and ready to use.
//
// `tryProxy` is true if the item is not in the local cache but can
// be requested from the proxy, in which case, a placeholder entry
// has been added to the index and the caller must either replace
// the entry with the actual size, or remove it from the LRU.
func (c *DiskCache) availableOrTryProxy(key string) (available bool, tryProxy bool) {
	inProgress := false
	tryProxy = false

	c.mu.Lock()

	existingItem, found := c.lru.Get(key)
	if found {
		if !existingItem.(*lruItem).committed {
			inProgress = true
		}
	} else if c.proxy != nil {
		// Reserve a place in the LRU.
		// The caller must replace or remove this!
		tryProxy = c.lru.Add(key, &lruItem{
			size:      0,
			committed: false,
		})
	}

	c.mu.Unlock()

	available = found && !inProgress

	return available, tryProxy
}

// Get returns an io.ReadCloser with the content of the cache item stored under `hash`
// and the number of bytes that can be read from it. If the item is not found, the
// io.ReadCloser will be nil. If some error occurred when processing the request, then
// it is returned.
func (c *DiskCache) Get(kind cache.EntryKind, hash string) (io.ReadCloser, int64, error) {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return nil, -1, fmt.Errorf("Invalid hash size: %d, expected: %d",
			len(hash), sha256.Size)
	}

	var err error
	key := cacheKey(kind, hash)

	available, tryProxy := c.availableOrTryProxy(key)

	if available {
		blobPath := cacheFilePath(kind, c.dir, hash)
		var fileInfo os.FileInfo
		fileInfo, err = os.Stat(blobPath)
		if err == nil {
			var f *os.File
			f, err = os.Open(blobPath)
			if err == nil {
				cacheHits.Inc()
				return f, fileInfo.Size(), nil
			}
		}

		cacheMisses.Inc()
		return nil, -1, err
	}

	cacheMisses.Inc()

	if !tryProxy {
		return nil, -1, nil
	}

	filePath := cacheFilePath(kind, c.dir, hash)
	tmpFilePath := filePath + ".tmp"
	shouldCommit := false
	tmpFileCreated := false
	foundSize := int64(-1)
	var f *os.File

	// We're allowed to try downloading this blob from the proxy.
	// Before returning, we have to either commit the item and set
	// its size, or remove the item from the LRU.
	defer func() {
		c.mu.Lock()

		if shouldCommit {
			// Overwrite the placeholder inserted by availableOrTryProxy.
			// Call Add instead of updating the entry directly, so we
			// update the currentSize value.
			c.lru.Add(key, &lruItem{
				size:      foundSize,
				committed: true,
			})
		} else {
			// Remove the placeholder.
			c.lru.Remove(key)
		}

		c.mu.Unlock()

		if !shouldCommit && tmpFileCreated {
			os.Remove(tmpFilePath) // No need to check the error.
		}

		f.Close() // No need to check the error.
	}()

	r, foundSize, err := c.proxy.Get(kind, hash)
	if r != nil {
		defer r.Close()
	}
	if err != nil || r == nil {
		return nil, -1, err
	}

	f, err = os.Create(tmpFilePath)
	if err != nil {
		return nil, -1, err
	}
	tmpFileCreated = true

	written, err := io.Copy(f, r)
	if err != nil {
		return nil, -1, err
	}

	if written != foundSize {
		return nil, -1, err
	}

	if err = f.Sync(); err != nil {
		return nil, -1, err
	}

	if err = f.Close(); err != nil {
		return nil, -1, err
	}

	// Rename to the final path
	err = os.Rename(tmpFilePath, filePath)
	if err == nil {
		// Only commit if renaming succeeded.
		// This flag is used by the defer() block above.
		shouldCommit = true

		var f2 *os.File
		f2, err = os.Open(filePath)
		if err == nil {
			return f2, foundSize, nil
		}
	}

	return nil, -1, err
}

// Contains returns true if the `hash` key exists in the cache, and
// the size if known (or -1 if unknown).
//
// If there is a local cache miss, the proxy backend (if there is
// one) will be checked.
func (c *DiskCache) Contains(kind cache.EntryKind, hash string) (bool, int64) {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return false, int64(-1)
	}

	var foundLocally bool
	size := int64(-1)

	c.mu.Lock()
	val, found := c.lru.Get(cacheKey(kind, hash))
	// Uncommitted (i.e. uploading items) should be reported as not ok
	if found {
		item := val.(*lruItem)
		foundLocally = item.committed
		size = item.size
	}
	c.mu.Unlock()

	if foundLocally {
		return true, size
	}

	if c.proxy != nil {
		return c.proxy.Contains(kind, hash)
	}

	return false, int64(-1)
}

// MaxSize returns the maximum cache size in bytes.
func (c *DiskCache) MaxSize() int64 {
	// The underlying value is never modified, no need to lock.
	return c.lru.MaxSize()
}

// Return the current size of the cache in bytes, and the number of
// items stored in the cache.
func (c *DiskCache) Stats() (currentSize int64, numItems int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lru.CurrentSize(), c.lru.Len()
}

func cacheKey(kind cache.EntryKind, hash string) string {
	return filepath.Join(kind.String(), hash[:2], hash)
}

func cacheFilePath(kind cache.EntryKind, cacheDir string, hash string) string {
	return filepath.Join(cacheDir, cacheKey(kind, hash))
}

// If `hash` refers to a valid ActionResult with all the dependencies
// available in the CAS, return it and its serialized value.
// If not, return nil values.
// If something unexpected went wrong, return an error.
func (c *DiskCache) GetValidatedActionResult(hash string) (*pb.ActionResult, []byte, error) {
	rdr, sizeBytes, err := c.Get(cache.AC, hash)
	if err != nil {
		return nil, nil, err
	}

	if rdr == nil || sizeBytes <= 0 {
		return nil, nil, nil // aka "not found"
	}

	acdata, err := ioutil.ReadAll(rdr)
	if err != nil {
		return nil, nil, err
	}

	result := &pb.ActionResult{}
	err = proto.Unmarshal(acdata, result)
	if err != nil {
		return nil, nil, err
	}

	for _, f := range result.OutputFiles {
		if len(f.Contents) == 0 && f.Digest.SizeBytes > 0 {
			found, _ := c.Contains(cache.CAS, f.Digest.Hash)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}
	}

	for _, d := range result.OutputDirectories {
		r, size, err := c.Get(cache.CAS, d.TreeDigest.Hash)
		if r == nil {
			return nil, nil, err // aka "not found", or an err if non-nil
		}
		if err != nil {
			r.Close()
			return nil, nil, err
		}
		if size != d.TreeDigest.SizeBytes {
			r.Close()
			return nil, nil, fmt.Errorf("expected %d bytes, found %d",
				d.TreeDigest.SizeBytes, size)
		}

		var oddata []byte
		oddata, err = ioutil.ReadAll(r)
		r.Close()
		if err != nil {
			return nil, nil, err
		}

		tree := pb.Tree{}
		err = proto.Unmarshal(oddata, &tree)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range tree.Root.GetFiles() {
			if f.Digest == nil {
				continue
			}
			found, _ := c.Contains(cache.CAS, f.Digest.Hash)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}

		for _, child := range tree.GetChildren() {
			for _, f := range child.GetFiles() {
				if f.Digest == nil {
					continue
				}
				found, _ := c.Contains(cache.CAS, f.Digest.Hash)
				if !found {
					return nil, nil, nil // aka "not found"
				}
			}
		}
	}

	if result.StdoutDigest != nil && result.StdoutDigest.SizeBytes > 0 {
		found, _ := c.Contains(cache.CAS, result.StdoutDigest.Hash)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	if result.StderrDigest != nil && result.StderrDigest.SizeBytes > 0 {
		found, _ := c.Contains(cache.CAS, result.StderrDigest.Hash)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	return result, acdata, nil
}
