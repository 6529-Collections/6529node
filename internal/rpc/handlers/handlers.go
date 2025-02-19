package handlers

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

type Method string
type Path string

var (
	HTTP_GET    Method = "GET"
	HTTP_POST   Method = "POST"
	HTTP_PUT    Method = "PUT"
	HTTP_DELETE Method = "DELETE"
)

func CreateApiV1Path(path string) Path {
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return Path("/api/v1/" + path)
}

type MethodHandlers map[Path]map[Method]func(r *http.Request) (any, error)

func SetupHandlers(mux *http.ServeMux, handlers MethodHandlers) {
	for path, methodHandlers := range handlers {
		mux.HandleFunc(string(path), func(w http.ResponseWriter, r *http.Request) {
			method := r.Method
			handler, ok := methodHandlers[Method(method)]
			if !ok {
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
			resp, err := handler(r)
			if err != nil {
				zap.L().Error("failed to handle request", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if resp != nil {
				err := json.NewEncoder(w).Encode(resp)
				if err != nil {
					zap.L().Error("failed to encode response", zap.Error(err))
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		})
	}
}
