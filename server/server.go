package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/cors"
)

// Handler is a handler.
type Handler struct {
	Path    string
	Handler http.Handler
}

// Server is a server.
type Server struct {
	cfg *Config
	mux *http.ServeMux
}

// New creates a new server.
func New(cfg *Config) *Server {
	return &Server{
		mux: http.NewServeMux(),
		cfg: cfg,
	}
}

// RegisterHandler registers a handler.
func (s *Server) RegisterHandler(h Handler) {
	s.mux.Handle(h.Path, h.Handler)
}

// Start starts the server.
func (s *Server) Start(_ context.Context) {
	handler := cors.AllowAll().Handler(s.mux) // yeet for now
	if err := http.ListenAndServe(            //nolint:gosec // its okay for now.
		fmt.Sprintf(":%d", s.cfg.HTTP.Port), handler); err != nil {
		panic(err)
	}
}

// Stop stops the server.
func (s *Server) Stop() {}
