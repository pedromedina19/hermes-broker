package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/config"
	"github.com/pedromedina19/hermes-broker/internal/core/metrics"
	"github.com/pedromedina19/hermes-broker/internal/core/services"
	"github.com/pedromedina19/hermes-broker/pb"
)

type RestHandler struct {
	service *services.BrokerService
	cfg     config.Config
}
type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}

type PublishBatchRequest struct {
	Topic    string   `json:"topic"`
	Payloads []string `json:"payloads"`
	Mode     int      `json:"mode"`
}
func NewHttpHandler(service *services.BrokerService, cfg config.Config) *RestHandler {
	return &RestHandler{
		service: service,
		cfg:     cfg,
	}
}

type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	Mode    int    `json:"mode"`
}

func (h *RestHandler) getLeaderHttpAddr() string {
	leaderRaftAddr := h.service.GetLeaderID()
	if leaderRaftAddr == "" {
		return ""
	}
	host := strings.Replace(leaderRaftAddr, "node-", "hermes-", 1)
	host = strings.Split(host, ":")[0] 
	
	if !strings.Contains(host, "hermes-internal") {
		host = fmt.Sprintf("%s.hermes-internal", host)
	}
	return fmt.Sprintf("%s:8080", host)
}

func (h *RestHandler) HandleGraphQL(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.service.IsLeader() {
			leaderAddr := h.getLeaderHttpAddr()
			if leaderAddr != "" {
				w.Header().Set("X-Leader-Hint", leaderAddr)
			}
		}
		next.ServeHTTP(w, r)
	}
}

func (h *RestHandler) HandlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.service.IsLeader() {
		leaderAddr := h.getLeaderHttpAddr()
		if leaderAddr != "" {
			w.Header().Set("X-Leader-Hint", leaderAddr)
		}
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

	deliveryMode := pb.DeliveryMode(req.Mode)

	err := h.service.Publish(r.Context(), req.Topic, []byte(req.Payload), deliveryMode)
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

func (h *RestHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	snapshot := metrics.GetSnapshot()
	
	res := map[string]interface{}{
		"node_id":    h.cfg.NodeID,
		"is_leader":  h.service.IsLeader(),
		"leader_id":  h.service.GetLeaderID(),
		"stats":      snapshot,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (h *RestHandler) HandlePublishBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Topic == "" || len(req.Payloads) == 0 {
		http.Error(w, "Topic and payloads are required", http.StatusBadRequest)
		return
	}

	deliveryMode := pb.DeliveryMode(req.Mode)

	payloads := make([][]byte, len(req.Payloads))
	for i, p := range req.Payloads {
		payloads[i] = []byte(p)
	}

	err := h.service.PublishBatchList(r.Context(), req.Topic, payloads, deliveryMode)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true, 
		"count": len(payloads),
	})
}