package handlers

import (
	"encoding/json"
	"fmt"
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

type ApiVersion string

const (
	ApiV1 ApiVersion = "v1"
)

var SupportedVersions = []ApiVersion{ApiV1}

type ApiEndpoint string

const (
	StatusEndpoint       ApiEndpoint = "status"
	NFTsEndpoint         ApiEndpoint = "nfts/"
	NFTTransfersEndpoint ApiEndpoint = "nft_transfers/"
	NFTOwnersEndpoint    ApiEndpoint = "nft_owners/"
)

func CreateApiPath(version ApiVersion, endpoint string) Path {
	if len(endpoint) > 0 && endpoint[0] == '/' {
		endpoint = endpoint[1:]
	}
	return Path(fmt.Sprintf("/api/%s/%s", version, endpoint))
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
				fmt.Println("i am error", err)
				if err.Error() == "not found" {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				zap.L().Error("failed to handle request", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if resp != nil {
				err := json.NewEncoder(w).Encode(resp)
				if err != nil {
					fmt.Println("i am error", err)
					zap.L().Error("failed to encode response", zap.Error(err))
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		})
	}
}
