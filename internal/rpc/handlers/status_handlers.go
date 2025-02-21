package handlers

import (
	"net/http"
)

type StatusResponse struct {
	Status string `json:"status"`
}

func StatusGetHandler(r *http.Request) (StatusResponse, error) {
	return StatusResponse{Status: "OK"}, nil
}
