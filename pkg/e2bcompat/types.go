package e2bcompat

// Field names and JSON casing are fixed by the e2b OpenAPI contract the SDKs unmarshal.

// Sandbox states reported to the SDK (spec: SandboxState).
const (
	StateRunning = "running"
	StatePaused  = "paused"
)

// NewSandbox is the POST /sandboxes request body (spec: NewSandbox). Only
// templateID is required; the rest carry SDK defaults.
type NewSandbox struct {
	TemplateID string `json:"templateID"`
	// Timeout is the sandbox time-to-live in seconds (SDK default 15).
	Timeout *int32 `json:"timeout,omitempty"`
	// Metadata is accepted for SDK compatibility but discarded: the node-local
	// claim path takes none of it and nothing here stores a per-sandbox copy.
	Metadata map[string]string `json:"metadata,omitempty"`
	// EnvVars, AutoPause and Secure name guarantees this backend cannot give,
	// so a request that asks for one is refused rather than quietly dropped.
	EnvVars   map[string]string `json:"envVars,omitempty"`
	AutoPause *bool             `json:"autoPause,omitempty"`
	Secure    *bool             `json:"secure,omitempty"`
	// AllowInternetAccess selects the warm pool's network lane: true picks the
	// egress-capable pool, false/nil the isolated one.
	AllowInternetAccess *bool `json:"allow_internet_access,omitempty"`
}

// Sandbox is the POST /sandboxes response (spec: Sandbox). templateID,
// sandboxID, clientID and envdVersion are required by the schema.
type Sandbox struct {
	TemplateID string `json:"templateID"`
	SandboxID  string `json:"sandboxID"`
	ClientID   string `json:"clientID"`
	// EnvdVersion gates SDK behavior (it version-compares before choosing the
	// envd auth style), so it is always reported.
	EnvdVersion     string `json:"envdVersion"`
	EnvdAccessToken string `json:"envdAccessToken,omitempty"`
	Domain          string `json:"domain,omitempty"`
	Alias           string `json:"alias,omitempty"`
}

// SandboxDetail is the GET /sandboxes/{sandboxID} response (spec:
// SandboxDetail), and its field set also satisfies ListedSandbox.
type SandboxDetail struct {
	TemplateID          string            `json:"templateID"`
	SandboxID           string            `json:"sandboxID"`
	ClientID            string            `json:"clientID"`
	StartedAt           string            `json:"startedAt"`
	EndAt               string            `json:"endAt"`
	State               string            `json:"state"`
	EnvdVersion         string            `json:"envdVersion"`
	CPUCount            int32             `json:"cpuCount"`
	MemoryMB            int32             `json:"memoryMB"`
	DiskSizeMB          int32             `json:"diskSizeMB"`
	Alias               string            `json:"alias,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
	EnvdAccessToken     string            `json:"envdAccessToken,omitempty"`
	Domain              string            `json:"domain,omitempty"`
	AllowInternetAccess *bool             `json:"allowInternetAccess,omitempty"`
}

// SandboxTimeoutRequest is the POST /sandboxes/{sandboxID}/timeout request body
// (spec: SandboxTimeoutRequest) — the SDK's setTimeout call.
type SandboxTimeoutRequest struct {
	Timeout int32 `json:"timeout"`
}

// SandboxRefreshRequest is the POST /sandboxes/{sandboxID}/refreshes body.
// Duration is optional; an absent or unrecognized one takes the server default.
type SandboxRefreshRequest struct {
	Duration *int32 `json:"duration,omitempty"`
}

// SandboxPauseRequest is the POST /sandboxes/{id}/pause body. memory=false
// takes a filesystem-only snapshot, whose resume cold-boots.
type SandboxPauseRequest struct {
	Memory *bool `json:"memory,omitempty"`
}

// ConnectSandbox is the POST /sandboxes/{id}/connect body — the SDK's resume.
// Timeout is required by the schema but does not change the node-owned lease.
type ConnectSandbox struct {
	Timeout int32 `json:"timeout"`
}

// SandboxForkRequest is the POST /sandboxes/{id}/fork body.
type SandboxForkRequest struct {
	Timeout *int32 `json:"timeout,omitempty"`
	Count   *int32 `json:"count,omitempty"`
}

// SandboxForkResult is one entry of the fork reply: exactly one of Sandbox or
// Error is set, so a partial failure still returns 201 with per-child detail.
type SandboxForkResult struct {
	Sandbox *Sandbox  `json:"sandbox,omitempty"`
	Error   *APIError `json:"error,omitempty"`
}

// SandboxSnapshotRequest is the POST /sandboxes/{id}/snapshots body.
type SandboxSnapshotRequest struct {
	Name string `json:"name,omitempty"`
}

// SnapshotInfo is the snapshot create/list reply.
type SnapshotInfo struct {
	SnapshotID string   `json:"snapshotID"`
	Names      []string `json:"names"`
}

// SandboxMetric is one metrics sample. Every field is required by the schema,
// so all are always emitted; the SDK reads the deprecated `timestamp`.
type SandboxMetric struct {
	Timestamp     string  `json:"timestamp"`
	TimestampUnix int64   `json:"timestampUnix"`
	CPUCount      int32   `json:"cpuCount"`
	CPUUsedPct    float32 `json:"cpuUsedPct"`
	MemUsed       int64   `json:"memUsed"`
	MemTotal      int64   `json:"memTotal"`
	MemCache      int64   `json:"memCache"`
	DiskUsed      int64   `json:"diskUsed"`
	DiskTotal     int64   `json:"diskTotal"`
}

// Template is the templates-listing entry.
type Template struct {
	TemplateID  string   `json:"templateID"`
	BuildID     string   `json:"buildID"`
	CPUCount    int32    `json:"cpuCount"`
	MemoryMB    int32    `json:"memoryMB"`
	DiskSizeMB  int32    `json:"diskSizeMB"`
	Public      bool     `json:"public"`
	Aliases     []string `json:"aliases"`
	Names       []string `json:"names"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
	SpawnCount  int64    `json:"spawnCount"`
	BuildCount  int32    `json:"buildCount"`
	EnvdVersion string   `json:"envdVersion"`
}

// APIError is the e2b error envelope. The SDK surfaces `message` on failures.
type APIError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}
