package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pedromedina19/hermes-broker/internal/core/services"
)

type RestHandler struct {
	service *services.BrokerService
}
type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
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

func (h *RestHandler) HandleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := h.service.JoinCluster(req.NodeID, req.RaftAddr); err != nil {
		http.Error(w, "Failed to join cluster: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *RestHandler) HandleSubscribeSSE(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	groupID := r.URL.Query().Get("group")

	if topic == "" {
		http.Error(w, "Topic query parameter is required", http.StatusBadRequest)
		return
	}

	// Configure Headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// created signature in the engine
	msgChan, subID, err := h.service.Subscribe(r.Context(), topic, groupID)
	if err != nil {
		http.Error(w, "Failed to subscribe: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer h.service.RemoveSubscriber(topic, subID)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "event: connected\ndata: {\"sub_id\": \"%s\"}\n\n", subID)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-msgChan:
			if !ok {
				return
			}

			data, _ := json.Marshal(map[string]interface{}{
				"id":        msg.ID,
				"topic":     msg.Topic,
				"payload":   string(msg.Payload),
				"timestamp": msg.Timestamp.Unix(),
			})

			// SSE format: "data: <content>\n\n"
			fmt.Fprintf(w, "data: %s\n\n", string(data))
			flusher.Flush()

			h.service.Acknowledge(subID, msg.ID)
		}
	}
}