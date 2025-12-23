package http

import (
	"encoding/json"
	"net/http"

	"github.com/pedromedina19/hermes-broker/internal/core/services"
)

type RestHandler struct {
	service *services.BrokerService
}

func NewRestHandler(service *services.BrokerService) *RestHandler {
	return &RestHandler{service: service}
}

type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

func (h *RestHandler) HandlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Payload == "" {
		http.Error(w, "Topic and payload are required", http.StatusBadRequest)
		return
	}

	err := h.service.Publish(r.Context(), req.Topic, []byte(req.Payload))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}