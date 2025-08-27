package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type ProcessInfo struct {
	PID     int    `json:"pid"`
	Name    string `json:"name"`
	Command string `json:"command"`
}

type KillRequest struct {
	PID int `json:"pid"`
}

type DeleteStreamRequest struct {
	StreamName string `json:"stream_name"`
}

type StreamMessage struct {
	Seq     uint64            `json:"seq"`
	Subject string            `json:"subject"`
	Headers map[string]string `json:"headers"`
	Data    string            `json:"data"`
	Time    string            `json:"time"`
}

func main() {
	r := mux.NewRouter()

	// Specific API routes MUST come before the catch-all static file handler
	r.HandleFunc("/api/processes", getProcesses).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/kill", killProcess).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/kv-leases", getKVLeases).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/kv-bucket-info", getKVBucketInfo).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/delete-stream", deleteStream).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/stream-messages", getStreamMessages).Methods("GET", "OPTIONS")

	// Proxy NATS monitoring API
	r.PathPrefix("/api/v1/").HandlerFunc(proxyToNATS).Methods("GET", "POST", "OPTIONS")

	// Serve static files from ui directory (this catches everything else)
	r.PathPrefix("/").Handler(http.FileServer(http.Dir("./ui/")))

	fmt.Println("Monitor server starting on :8082")
	fmt.Println("UI available at http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", corsMiddleware(r)))
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers for all requests
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func getProcesses(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	processes, err := findConsumerProcesses()
	if err != nil {
		http.Error(w, "Failed to get processes: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(processes)
}

func killProcess(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	var req KillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.PID <= 0 {
		http.Error(w, "Invalid PID", http.StatusBadRequest)
		return
	}

	// Kill the process
	cmd := exec.Command("kill", "-9", strconv.Itoa(req.PID))
	if err := cmd.Run(); err != nil {
		http.Error(w, "Failed to kill process: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("Killed process %d\n", req.PID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Process %d killed successfully", req.PID)})
}

func getKVLeases(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	// Use NATS CLI via docker to get KV bucket keys
	cmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "kv", "ls", "leases", "--server", "nats:4222")
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, "Failed to get KV leases: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse the output into key-value entries
	lines := strings.Split(string(output), "\n")
	var kvEntries []map[string]interface{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Key") || strings.HasPrefix(line, "---") {
			continue
		}

		// Get the value for each key
		parts := strings.Fields(line)
		if len(parts) > 0 {
			key := parts[0]

			// Get the value for this key
			valueCmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "kv", "get", "leases", key, "--server", "nats:4222")
			valueOutput, valueErr := valueCmd.Output()

			var value interface{}
			if valueErr == nil {
				// Try to parse as JSON
				if err := json.Unmarshal(valueOutput, &value); err != nil {
					// If not JSON, store as string
					value = string(valueOutput)
				}
			} else {
				value = "Error reading value"
			}

			kvEntries = append(kvEntries, map[string]interface{}{
				"key":   key,
				"value": value,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(kvEntries)
}

func getKVBucketInfo(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	// Get KV bucket info using NATS CLI
	cmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "kv", "info", "leases", "--json", "--server", "nats:4222")
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, "Failed to get KV bucket info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse the JSON output
	var bucketInfo map[string]interface{}
	if err := json.Unmarshal(output, &bucketInfo); err != nil {
		http.Error(w, "Failed to parse KV bucket info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bucketInfo)
}

func handleOptions(w http.ResponseWriter, r *http.Request) {
	// Handle preflight requests
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.WriteHeader(http.StatusOK)
}

func deleteStream(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	var req DeleteStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.StreamName == "" {
		http.Error(w, "Stream name is required", http.StatusBadRequest)
		return
	}

	// Delete the stream using NATS CLI
	cmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "stream", "rm", req.StreamName, "--force", "--server", "nats:4222")
	output, err := cmd.CombinedOutput()
	if err != nil {
		http.Error(w, "Failed to delete stream: "+err.Error()+"\nOutput: "+string(output), http.StatusInternalServerError)
		return
	}

	fmt.Printf("Deleted stream %s\n", req.StreamName)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Stream %s deleted successfully", req.StreamName)})
}

func getStreamMessages(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers explicitly
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	streamName := r.URL.Query().Get("stream")
	if streamName == "" {
		streamName = "stream_name" // default stream
	}

	// Get the last sequence number
	cmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "stream", "info", streamName, "--json", "--server", "nats:4222")
	output, err := cmd.Output()
	if err != nil {
		http.Error(w, "Failed to get stream info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var streamInfo map[string]interface{}
	if err := json.Unmarshal(output, &streamInfo); err != nil {
		http.Error(w, "Failed to parse stream info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	state, ok := streamInfo["state"].(map[string]interface{})
	if !ok {
		http.Error(w, "Failed to parse stream state", http.StatusInternalServerError)
		return
	}

	lastSeq, ok := state["last_seq"].(float64)
	if !ok {
		http.Error(w, "Failed to get last sequence number", http.StatusInternalServerError)
		return
	}

	// Get the last 10 messages
	var messages []StreamMessage
	for i := 0; i < 10; i++ {
		seq := uint64(lastSeq) - uint64(i)
		if seq <= 0 {
			break
		}

		// Get message by sequence number
		cmd := exec.Command("docker", "exec", "nats-poc-nats-box-1", "nats", "stream", "get", streamName, fmt.Sprintf("%d", seq), "--server", "nats:4222")
		output, err := cmd.Output()
		if err != nil {
			// Skip if message doesn't exist
			continue
		}

		// Parse the output
		lines := strings.Split(string(output), "\n")
		msg := StreamMessage{
			Seq:     seq,
			Headers: make(map[string]string),
		}

		for j, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// Look for the item line
			if strings.HasPrefix(line, "Item:") {
				// Parse subject
				subjectIdx := strings.Index(line, "on Subject ")
				if subjectIdx > 0 {
					msg.Subject = line[subjectIdx+11:] // 11 = len("on Subject ")
				}

				// Parse time
				timeIdx := strings.Index(line, "received ")
				if timeIdx > 0 {
					timeEndIdx := strings.Index(line[timeIdx:], " on Subject")
					if timeEndIdx > 0 {
						msg.Time = line[timeIdx+9 : timeIdx+timeEndIdx] // 9 = len("received ")
					}
				}
			} else if line == "Headers:" {
				// Skip headers line, process next lines as headers
				continue
			} else if strings.HasPrefix(line, "  ") && strings.Contains(line, ":") {
				// This is a header line
				headerParts := strings.SplitN(line[2:], ":", 2)
				if len(headerParts) == 2 {
					msg.Headers[strings.TrimSpace(headerParts[0])] = strings.TrimSpace(headerParts[1])
				}
			} else if line != "" && !strings.HasPrefix(line, "Headers:") && j > 0 {
				// This is the message data (everything after headers)
				if strings.HasPrefix(lines[j-1], "  ") || lines[j-1] == "Headers:" || strings.HasPrefix(lines[j-1], "Item:") {
					// This is the start of data
					msg.Data = line
				} else {
					// Append to existing data
					msg.Data += "\n" + line
				}
			}
		}

		messages = append(messages, msg)
	}

	// Reverse the order to show newest first
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

func findConsumerProcesses() ([]ProcessInfo, error) {
	var processes []ProcessInfo

	// Use ps to find consumer processes
	cmd := exec.Command("ps", "aux")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		// Look for consumer processes
		if strings.Contains(line, "./bin/consumer") || strings.Contains(line, "consumer-") {
			fields := strings.Fields(line)
			if len(fields) >= 11 {
				pidStr := fields[1]
				pid, err := strconv.Atoi(pidStr)
				if err != nil {
					continue
				}

				// Extract consumer name from command line
				command := strings.Join(fields[10:], " ")
				name := "consumer"

				// Try to extract worker name from NATS connection name
				if strings.Contains(command, "consumer-") {
					parts := strings.Split(command, "consumer-")
					if len(parts) > 1 {
						name = "consumer-" + strings.Fields(parts[1])[0]
					}
				}

				processes = append(processes, ProcessInfo{
					PID:     pid,
					Name:    name,
					Command: command,
				})
			}
		}
	}

	return processes, nil
}

func proxyToNATS(w http.ResponseWriter, r *http.Request) {
	// Forward requests to NATS monitoring API
	natsURL := "http://localhost:8222" + r.URL.Path
	if r.URL.RawQuery != "" {
		natsURL += "?" + r.URL.RawQuery
	}

	proxyURL, err := url.Parse(natsURL)
	if err != nil {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	// Create request to NATS server
	req, err := http.NewRequest(r.Method, proxyURL.String(), r.Body)
	if err != nil {
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	// Make request to NATS server
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "Failed to proxy request: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}
