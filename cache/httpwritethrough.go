package cache

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

const numUploaders = 100
const maxQueuedUploads = 10000

type uploadReq struct {
	key string
	ac  bool
}

type remoteHTTPCache struct {
	remote       *http.Client
	baseURL      string
	local        Cache
	uploadQueue  chan<- (*uploadReq)
	accessLogger Logger
	errorLogger  Logger
}

func uploadFile(remote *http.Client, baseURL string, local Cache, accessLogger Logger,
	errorLogger Logger, key string, ac bool) {
	data, size, err := local.Get(key, ac)
	if err != nil {
		return
	}

	if size == 0 {
		// See https://github.com/golang/go/issues/20257#issuecomment-299509391
		data = http.NoBody
	}
	url := requestURL(baseURL, key, ac)
	req, err := http.NewRequest(http.MethodPut, url, data)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size

	rsp, err := remote.Do(req)
	if err != nil {
		return
	}
	logResponse(accessLogger, "PUT", rsp.StatusCode, url)
	return
}

// NewRemoteHTTPCache ...
func NewRemoteHTTPCache(baseURL string, local Cache, remote *http.Client, accessLogger Logger,
	errorLogger Logger) Cache {
	uploadQueue := make(chan *uploadReq, maxQueuedUploads)
	for uploader := 0; uploader < numUploaders; uploader++ {
		go func(remote *http.Client, baseURL string, local Cache, accessLogger Logger,
			errorLogger Logger) {
			for item := range uploadQueue {
				uploadFile(remote, baseURL, local, accessLogger, errorLogger, item.key, item.ac)
			}
		}(remote, baseURL, local, accessLogger, errorLogger)
	}
	return &remoteHTTPCache{
		remote:       remote,
		baseURL:      baseURL,
		local:        local,
		uploadQueue:  uploadQueue,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}
}

// Helper function for logging responses
func logResponse(log Logger, method string, code int, url string) {
	log.Printf("%4s %d %15s %s", method, code, "", url)
}

func (r *remoteHTTPCache) Put(key string, size int64, expectedSha256 string, data io.Reader) (err error) {
	fromActionCache := expectedSha256 == ""
	if r.local.Contains(key, fromActionCache) {
		io.Copy(ioutil.Discard, data)
		return nil
	}
	r.local.Put(key, size, expectedSha256, data)

	select {
	case r.uploadQueue <- &uploadReq{
		key: key,
		ac:  fromActionCache,
	}:
	default:
		r.errorLogger.Printf("too many uploads queued")
	}
	return
}

func (r *remoteHTTPCache) Get(key string, fromActionCache bool) (data io.ReadCloser, sizeBytes int64, err error) {
	if r.local.Contains(key, fromActionCache) {
		return r.local.Get(key, fromActionCache)
	}

	url := requestURL(r.baseURL, key, fromActionCache)
	rsp, err := r.remote.Get(url)
	if err != nil {
		return
	}
	defer rsp.Body.Close()

	logResponse(r.accessLogger, "GET", rsp.StatusCode, url)

	if rsp.StatusCode != http.StatusOK {
		return
	}

	sizeBytesStr := rsp.Header.Get("Content-Length")
	if sizeBytesStr == "" {
		err = errors.New("Missing Content-Length header")
		return
	}
	sizeBytesInt, err := strconv.Atoi(sizeBytesStr)
	if err != nil {
		return
	}
	sizeBytes = int64(sizeBytesInt)

	err = r.local.Put(key, sizeBytes, "", rsp.Body)
	if err != nil {
		return
	}

	return r.local.Get(key, fromActionCache)
}

func (r *remoteHTTPCache) Contains(key string, fromActionCache bool) (ok bool) {
	return r.local.Contains(key, fromActionCache)
}

func (r *remoteHTTPCache) MaxSize() int64 {
	return r.local.MaxSize()
}

func (r *remoteHTTPCache) CurrentSize() int64 {
	return r.local.CurrentSize()
}

func (r *remoteHTTPCache) NumItems() int {
	return r.local.NumItems()
}

func requestURL(baseURL string, key string, fromActionCache bool) string {
	url := baseURL
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	url += key
	return url
}
