package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"

	auth "github.com/abbot/go-http-auth"
	"github.com/buchgr/bazel-remote/cache"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func main() {
	host := flag.String("host", "", "Address to listen on. Listens on all network interfaces by default.")
	port := flag.Int("port", 8080, "The port the HTTP server listens on")
	dir := flag.String("dir", "",
		"Directory path where to store the cache contents. This flag is required.")
	maxSize := flag.Int64("max_size", -1,
		"The maximum size of the remote cache in GiB. This flag is required.")
	htpasswdFile := flag.String("htpasswd_file", "", "Path to a .htpasswd file. This flag is optional. Please read https://httpd.apache.org/docs/2.4/programs/htpasswd.html.")
	tlsEnabled := flag.Bool("tls_enabled", false, "Bool specifying whether or not to start the server with tls.  If true, server_cert and server_key flags are requred.")
	tlsCertFile := flag.String("tls_cert_file", "", "Path to a PEM encoded certificate file.  Required if tls_enabled is set to true.")
	tlsKeyFile := flag.String("tls_key_file", "", "Path to a PEM encoded key file.  Required if tls_enabled is set to true.")
	remoteHTTPCache := flag.String("remote_http_cache", "", "")

	flag.Parse()

	if *dir == "" || *maxSize <= 0 {
		flag.Usage()
		return
	}

	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)
	accessLogger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.LUTC)
	errorLogger := log.New(os.Stderr, "", log.Ldate|log.Ltime|log.LUTC)

	fsCache := cache.NewFsCache(*dir, *maxSize*1024*1024*1024)
	cacheBackend := fsCache
	if *remoteHTTPCache != "" {
		remoteClient, err := google.DefaultClient(oauth2.NoContext,
			"https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			log.Fatal(err)
		}
		cacheBackend = cache.NewRemoteHTTPCache(*remoteHTTPCache, fsCache, remoteClient,
			accessLogger, errorLogger)
	}
	h := cache.NewHTTPCache(cacheBackend, accessLogger, errorLogger)

	http.HandleFunc("/status", h.StatusPageHandler)
	http.HandleFunc("/", maybeAuth(h.CacheHandler, *htpasswdFile, *host))
	var serverErr error

	if *tlsEnabled {
		if len(*tlsCertFile) < 1 || len(*tlsKeyFile) < 1 {
			flag.Usage()
			return
		}
		serverErr = http.ListenAndServeTLS(*host+":"+strconv.Itoa(*port), *tlsCertFile, *tlsKeyFile, nil)
	} else {
		serverErr = http.ListenAndServe(*host+":"+strconv.Itoa(*port), nil)
	}
	if serverErr != nil {
		log.Fatal("ListenAndServe: ", serverErr)
	}
}

func maybeAuth(fn http.HandlerFunc, htpasswdFile string, host string) http.HandlerFunc {
	if htpasswdFile != "" {
		secrets := auth.HtpasswdFileProvider(htpasswdFile)
		authenticator := auth.NewBasicAuthenticator(host, secrets)
		return auth.JustCheck(authenticator, fn)
	}
	return fn
}
