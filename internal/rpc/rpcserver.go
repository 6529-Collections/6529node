package rpc

import (
	"context"
	"database/sql"
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

func StartRPCServer(port int, sqlite *sql.DB, ctx context.Context) func() {
	zap.L().Info("Starting RPC server on port", zap.Int("port", port))
	mux := http.NewServeMux()

	apiHandlers := make(handlers.MethodHandlers)

	// add status endpoint for all supported versions
	for _, version := range handlers.SupportedVersions {
		path := handlers.CreateApiPath(version, string(handlers.StatusEndpoint))
		apiHandlers[path] = map[handlers.Method]func(r *http.Request) (any, error){
			handlers.HTTP_GET: func(r *http.Request) (any, error) {
				return handlers.StatusGetHandler(r)
			},
		}
	}

	// /nfts/*
	nftV1WildcardPath := handlers.CreateApiPath(handlers.ApiV1, string(handlers.NFTsEndpoint))
	apiHandlers[nftV1WildcardPath] = map[handlers.Method]func(r *http.Request) (any, error){
		handlers.HTTP_GET: func(r *http.Request) (any, error) {
			return handlers.NFTsGetHandler(r, sqlite)
		},
	}

	// /nft_transfers/*
	nftTransferV1WildcardPath := handlers.CreateApiPath(handlers.ApiV1, string(handlers.NFTTransfersEndpoint))
	apiHandlers[nftTransferV1WildcardPath] = map[handlers.Method]func(r *http.Request) (any, error){
		handlers.HTTP_GET: func(r *http.Request) (any, error) {
			return handlers.NFTTransfersGetHandler(r, sqlite)
		},
	}

	// /nft_owners/*
	nftOwnerV1WildcardPath := handlers.CreateApiPath(handlers.ApiV1, string(handlers.NFTOwnersEndpoint))
	apiHandlers[nftOwnerV1WildcardPath] = map[handlers.Method]func(r *http.Request) (any, error){
		handlers.HTTP_GET: func(r *http.Request) (any, error) {
			return handlers.NFTOwnersGetHandler(r, sqlite)
		},
	}

	handlers.SetupHandlers(mux, apiHandlers)

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

// loggingMiddleware logs details about incoming HTTP requests.
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
