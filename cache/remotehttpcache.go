package cache

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type remoteHTTPCache struct {
	remote       *http.Client
	baseURL      string
	local        Cache
	accessLogger logger
	errorLogger  logger
}

// NewRemoteHTTPCache ...
func NewRemoteHTTPCache(baseURL string, local Cache, accessLogger logger, errorLogger logger) Cache {
	//remote, _ := google.DefaultClient(oauth2.NoContext, "https://www.googleapis.com/auth/cloud-platform")
	return &remoteHTTPCache{
		remote:       http.DefaultClient,
		baseURL:      baseURL,
		local:        local,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}
}

func (r *remoteHTTPCache) Put(key string, size int64, expectedSha256 string, data io.Reader) (err error) {
	fromActionCache := expectedSha256 == ""
	if r.local.Contains(key, fromActionCache) {
		io.Copy(ioutil.Discard, data)
		return nil
	}
	r.local.Put(key, size, expectedSha256, data)

	data, size, err = r.local.Get(key, fromActionCache)
	if err != nil {
		return
	}

	url := requestURL(r.baseURL, key, fromActionCache)
	req, err := http.NewRequest(http.MethodPut, url, data)
	if err != nil {
		return
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	rsp, err := r.remote.Do(req)
	if err != nil {
		return
	}
	if rsp.StatusCode != http.StatusOK {
		err = fmt.Errorf("PUT '%d' for '%s'", rsp.StatusCode, url)
		return
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

	r.accessLogger.Printf("GET %d '%s'", rsp.StatusCode, url)

	if rsp.StatusCode == http.StatusNotFound {
		return
	} else if rsp.StatusCode != http.StatusOK {
		err = fmt.Errorf("'%s' responded with status '%d'", url, rsp.StatusCode)
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
