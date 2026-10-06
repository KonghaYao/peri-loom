package platform

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/*
var adminFiles embed.FS

func AdminHandler() http.Handler {
	assets, err := fs.Sub(adminFiles, "web")
	if err != nil {
		panic(err)
	}
	server := http.FileServer(http.FS(assets))
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("Referrer-Policy", "no-referrer")
		response.Header().Set("Cache-Control", "no-store")
		server.ServeHTTP(response, request)
	})
}
