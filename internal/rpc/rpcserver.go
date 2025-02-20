package rpc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/6529-Collections/6529node/internal/rpc/handlers"
	"go.uber.org/zap"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func StartRPCServer(port int, ctx context.Context) func() {
	zap.L().Info("Starting RPC server on port", zap.Int("port", port))
	mux := http.NewServeMux()

	handlers.SetupHandlers(mux, handlers.MethodHandlers{
		handlers.CreateApiV1Path("status"): {
			handlers.HTTP_GET: func(r *http.Request) (any, error) {
				return handlers.StatusGetHandler(r)
			},
		},
	})

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr:    addr,
		Handler: loggingMiddleware(mux),
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				zap.L().Info("RPC server closed")
			} else {
				zap.L().Fatal("starting RPC server failed", zap.Error(err))
			}
		}
	}()
	closeFunc := func() {
		zap.L().Info("Closing RPC server...")
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			zap.L().Error("server shutdown failed", zap.Error(err))
		}
	}
	return closeFunc
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &responseWriter{w, http.StatusOK}
		next.ServeHTTP(rw, r)

		zap.L().Info("Request",
			zap.String("ip", r.RemoteAddr),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", rw.statusCode),
		)
	})
}
