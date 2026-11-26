package api

import (
	"net/http"
	"strings"
)

// CORS for the API docs site («Try it out» in https://api-docs.kub.k2.lan): only the one exact
// origin from API_DOCS_ORIGIN gets Access-Control-Allow-Origin, with credentials, and only on the
// API routes — never on Swagger UI (/docs, /docs/…) or the spec. No wildcard, no pattern, no
// echoing of an arbitrary Origin. Empty origin: no CORS, as before.
const (
	corsAllowMethods = "GET, POST, OPTIONS"
	corsAllowHeaders = "content-type, authorization, x-csrf-token, x-requested-with"
	corsMaxAge       = "600"
)

// isAPIPath is true for every route but the docs pages and the spec (path without PATH_BASE).
func isAPIPath(p string) bool {
	return p != "/docs" && p != "/openapi.json" && !strings.HasPrefix(p, "/docs/")
}

func cors(origin string, next http.Handler) http.Handler {
	if origin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isAPIPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		h := w.Header()
		h.Add("Vary", "Origin")
		if r.Header.Get("Origin") != origin {
			next.ServeHTTP(w, r)
			return
		}
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Credentials", "true")
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			h.Set("Access-Control-Allow-Methods", corsAllowMethods)
			h.Set("Access-Control-Allow-Headers", corsAllowHeaders)
			h.Set("Access-Control-Max-Age", corsMaxAge)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
