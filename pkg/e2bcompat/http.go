package e2bcompat

import (
	"encoding/json"
	"net/http"
)

const (
	// maxBodyBytes caps a request body. The e2b bodies are small objects; the
	// cap keeps an unbounded upload off the claim path.
	maxBodyBytes = 1 << 20
	// namePrefix prefixes the Kubernetes object name of a compat claim, so a
	// sandbox created through this surface is recognizable in `kubectl get
	// sandboxes` and in node inventory.
	namePrefix = "e2b-"
)

// writeJSON writes v as the response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The body is already committed by WriteHeader, so an encode failure can
	// only be logged by the caller's transport; there is no status left to set.
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes the e2b error envelope, which the SDK surfaces as the
// failure message.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, APIError{Code: int32(status), Message: msg})
}
