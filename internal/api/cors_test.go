package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	docsOrigin = "https://api-docs.kub.k2.lan"
	evilOrigin = "https://evil.kub.k2.lan"
)

// corsHandler is the real middleware order (PATH_BASE stripped first) over stand-in routes.
func corsHandler(origin string) http.Handler {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("POST /messages/chat/send", ok)
	mux.HandleFunc("GET /chats", ok)
	mux.HandleFunc("GET /openapi.json", ok)
	mux.HandleFunc("GET /docs/{$}", ok)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
	return pathBase("/whatsapp", cors(origin, mux))
}

func do(h http.Handler, method, path, origin string, preflight bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if preflight {
		r.Header.Set("Access-Control-Request-Method", "POST")
		r.Header.Set("Access-Control-Request-Headers", "content-type")
	}
	r.AddCookie(&http.Cookie{Name: "session", Value: "x"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCORSPreflightFromDocs(t *testing.T) {
	w := do(corsHandler(docsOrigin), http.MethodOptions, "/whatsapp/messages/chat/send", docsOrigin, true)
	h := w.Header()
	if w.Code != http.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	if h.Get("Access-Control-Allow-Origin") != docsOrigin || h.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("allow-origin/credentials: %v", h)
	}
	if h.Get("Access-Control-Allow-Methods") != "GET, POST, OPTIONS" || h.Get("Access-Control-Max-Age") != "600" {
		t.Fatalf("methods/max-age: %v", h)
	}
	for _, want := range []string{"content-type", "authorization", "x-csrf-token", "x-requested-with"} {
		if !strings.Contains(h.Get("Access-Control-Allow-Headers"), want) {
			t.Fatalf("allow-headers lacks %s: %q", want, h.Get("Access-Control-Allow-Headers"))
		}
	}
	if !strings.Contains(strings.Join(h.Values("Vary"), ","), "Origin") {
		t.Fatalf("no Vary: Origin")
	}
}

func TestCORSRequestFromDocsWithCookie(t *testing.T) {
	for _, path := range []string{"/whatsapp/messages/chat/send", "/messages/chat/send"} {
		w := do(corsHandler(docsOrigin), http.MethodPost, path, docsOrigin, false)
		h := w.Header()
		if w.Code != http.StatusOK || h.Get("Access-Control-Allow-Origin") != docsOrigin ||
			h.Get("Access-Control-Allow-Credentials") != "true" || h.Get("Vary") != "Origin" {
			t.Fatalf("%s: %d %v", path, w.Code, h)
		}
	}
}

func TestCORSOtherOriginGetsNothing(t *testing.T) {
	origins := []string{evilOrigin, "http://api-docs.kub.k2.lan", docsOrigin + ":8443", "https://api-docs.kub.k2.lan.evil.net", "null"}
	for _, origin := range origins {
		for _, preflight := range []bool{true, false} {
			method := http.MethodPost
			if preflight {
				method = http.MethodOptions
			}
			w := do(corsHandler(docsOrigin), method, "/whatsapp/messages/chat/send", origin, preflight)
			h := w.Header()
			if h.Get("Access-Control-Allow-Origin") != "" || h.Get("Access-Control-Allow-Credentials") != "" ||
				h.Get("Access-Control-Allow-Methods") != "" {
				t.Fatalf("%s preflight=%v: %v", origin, preflight, h)
			}
			if preflight && w.Code == http.StatusNoContent {
				t.Fatalf("%s: preflight answered", origin)
			}
		}
	}
}

func TestCORSNotOnDocsOrSpec(t *testing.T) {
	for _, path := range []string{"/whatsapp/docs/", "/whatsapp/openapi.json"} {
		w := do(corsHandler(docsOrigin), http.MethodGet, path, docsOrigin, false)
		if w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("%s got CORS", path)
		}
	}
}

func TestCORSOffWithoutOrigin(t *testing.T) {
	h := corsHandler("")
	pre := do(h, http.MethodOptions, "/whatsapp/messages/chat/send", docsOrigin, true)
	if pre.Header().Get("Access-Control-Allow-Origin") != "" || pre.Code == http.StatusNoContent {
		t.Fatalf("preflight: %d %v", pre.Code, pre.Header())
	}
	if w := do(h, http.MethodGet, "/whatsapp/chats", docsOrigin, false); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("get: %v", w.Header())
	}
}
