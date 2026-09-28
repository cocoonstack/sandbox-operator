package e2bcompat

import (
	"encoding/json"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
)

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
	// Metadata rides on the claim; list and detail report it and list filters on it.
	Metadata map[string]string `json:"metadata,omitempty"`
	// AutoPause archives the sandbox at lease end instead of destroying it; the internet lane refuses it.
	AutoPause *bool `json:"autoPause,omitempty"`
	// EnvVars becomes envd's default environment for every process the sandbox starts.
	EnvVars map[string]string `json:"envVars,omitempty"`
	// Secure=false asks for a sandbox reachable without its token, which this backend never hands out.
	Secure *bool `json:"secure,omitempty"`
	// AllowInternetAccess selects the warm pool's network lane: true picks the
	// egress-capable pool, false/nil the isolated one.
	AllowInternetAccess *bool `json:"allow_internet_access,omitempty"`

	Network         map[string]json.RawMessage `json:"network,omitempty"`
	VolumeMounts    []json.RawMessage          `json:"volumeMounts,omitempty"`
	AutoPauseMemory *bool                      `json:"autoPauseMemory,omitempty"`
	AutoResume      *struct {
		Enabled bool `json:"enabled"`
	} `json:"autoResume,omitempty"`
	MCP map[string]json.RawMessage `json:"mcp,omitempty"`
	IAM map[string]json.RawMessage `json:"iam,omitempty"`
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
	TemplateID      string          `json:"templateID"`
	SandboxID       string          `json:"sandboxID"`
	ClientID        string          `json:"clientID"`
	StartedAt       string          `json:"startedAt"`
	EndAt           string          `json:"endAt"`
	State           string          `json:"state"`
	EnvdVersion     string          `json:"envdVersion"`
	CPUCount        int32           `json:"cpuCount"`
	MemoryMB        int32           `json:"memoryMB"`
	DiskSizeMB      int32           `json:"diskSizeMB"`
	Alias           string          `json:"alias,omitempty"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
	EnvdAccessToken string          `json:"envdAccessToken,omitempty"`
	Domain          string          `json:"domain,omitempty"`

	startedAtKey string
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

// ConnectSandbox is the POST /sandboxes/{id}/connect body — the SDK's resume;
// Timeout extends the lease to that many seconds from now, never shortens it.
type ConnectSandbox struct {
	Timeout *int32 `json:"timeout,omitempty"`
	Memory  *bool  `json:"memory,omitempty"`
}

// ResumedSandbox is the legacy POST /sandboxes/{id}/resume body (spec: ResumedSandbox).
type ResumedSandbox struct {
	ConnectSandbox
	AutoPause *bool `json:"autoPause,omitempty"`
}

// SandboxForkRequest is the POST /sandboxes/{id}/fork body.
type SandboxForkRequest struct {
	Timeout *int32 `json:"timeout,omitempty"`
	Count   *int32 `json:"count,omitempty"`
}

// SandboxForkResult is one entry of the fork reply in e2b's shape; a fork here is
// all-or-nothing, so Error is never set and a failed child fails the whole with 500.
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

// SandboxesWithMetrics is the GET /sandboxes/metrics reply, keyed by sandbox id.
type SandboxesWithMetrics struct {
	Sandboxes map[string]SandboxMetric `json:"sandboxes"`
}

// SandboxLogs is the GET /sandboxes/{id}/logs reply (spec: SandboxLogs).
type SandboxLogs struct {
	Logs       []struct{} `json:"logs"`
	LogEntries []struct{} `json:"logEntries"`
}

// SandboxLogsV2 is the GET /v2/sandboxes/{id}/logs reply (spec: SandboxLogsV2Response).
type SandboxLogsV2 struct {
	Logs []struct{} `json:"logs"`
}

// Template is the templates-listing entry; createdBy and lastSpawnedAt are always null here.
type Template struct {
	TemplateID    string    `json:"templateID"`
	BuildID       string    `json:"buildID"`
	CPUCount      int32     `json:"cpuCount"`
	MemoryMB      int32     `json:"memoryMB"`
	DiskSizeMB    int32     `json:"diskSizeMB"`
	Public        bool      `json:"public"`
	Aliases       []string  `json:"aliases"`
	Names         []string  `json:"names"`
	CreatedAt     string    `json:"createdAt"`
	UpdatedAt     string    `json:"updatedAt"`
	CreatedBy     *struct{} `json:"createdBy"`
	LastSpawnedAt *string   `json:"lastSpawnedAt"`
	SpawnCount    int64     `json:"spawnCount"`
	BuildCount    int32     `json:"buildCount"`
	EnvdVersion   string    `json:"envdVersion"`
	BuildStatus   string    `json:"buildStatus"`
}

// TemplateWithBuilds is the GET /templates/{templateID} reply (spec: TemplateWithBuilds).
type TemplateWithBuilds struct {
	TemplateID    string          `json:"templateID"`
	Public        bool            `json:"public"`
	Aliases       []string        `json:"aliases"`
	Names         []string        `json:"names"`
	CreatedAt     string          `json:"createdAt"`
	UpdatedAt     string          `json:"updatedAt"`
	LastSpawnedAt *string         `json:"lastSpawnedAt"`
	SpawnCount    int64           `json:"spawnCount"`
	Builds        []TemplateBuild `json:"builds"`
}

// TemplateBuild is one build of a template; here one per content digest the fleet reports.
type TemplateBuild struct {
	BuildID     string `json:"buildID"`
	Status      string `json:"status"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
	FinishedAt  string `json:"finishedAt"`
	CPUCount    int32  `json:"cpuCount"`
	MemoryMB    int32  `json:"memoryMB"`
	EnvdVersion string `json:"envdVersion"`
}

// TemplateUpdateRequest is the PATCH /templates/{templateID} body; public is accepted and ignored.
type TemplateUpdateRequest struct {
	Public *bool `json:"public,omitempty"`
}

// AssignTemplateTagsRequest is the POST /templates/tags body; target is "name" or "name:tag".
type AssignTemplateTagsRequest struct {
	Target string   `json:"target"`
	Tags   []string `json:"tags"`
}

// AssignedTemplateTags is the POST /templates/tags reply.
type AssignedTemplateTags struct {
	Tags    []string `json:"tags"`
	BuildID string   `json:"buildID"`
}

// DeleteTemplateTagsRequest is the DELETE /templates/tags body.
type DeleteTemplateTagsRequest struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// TemplateTag is one tag of a template (spec: TemplateTag).
type TemplateTag struct {
	Tag       string `json:"tag"`
	BuildID   string `json:"buildID"`
	CreatedAt string `json:"createdAt"`
}

// TemplateBuildRequestV3 is the POST /v3/templates body; name may carry a ":tag", and cpuCount/memoryMB pick the size class.
type TemplateBuildRequestV3 struct {
	Name     string   `json:"name"`
	Alias    string   `json:"alias"`
	Tags     []string `json:"tags"`
	CPUCount int32    `json:"cpuCount"`
	MemoryMB int32    `json:"memoryMB"`
}

// TemplateRequestResponseV3 is the POST /v3/templates reply.
type TemplateRequestResponseV3 struct {
	TemplateID string   `json:"templateID"`
	BuildID    string   `json:"buildID"`
	Public     bool     `json:"public"`
	Names      []string `json:"names"`
	Tags       []string `json:"tags"`
	Aliases    []string `json:"aliases"`
}

// TemplateBuildStartV2 is the POST /v2/templates/{templateID}/builds/{buildID} body; fromTemplate and fromImageRegistry are not served.
type TemplateBuildStartV2 struct {
	FromImage         string          `json:"fromImage"`
	FromTemplate      string          `json:"fromTemplate"`
	FromImageRegistry json.RawMessage `json:"fromImageRegistry"`
	Steps             []e2bbuild.Step `json:"steps"`
	StartCmd          string          `json:"startCmd"`
	ReadyCmd          string          `json:"readyCmd"`
}

// TemplateBuildFileUpload is the GET /templates/{templateID}/files/{hash} reply: the archive is present, or the SDK PUTs it to URL with Headers.
type TemplateBuildFileUpload struct {
	Present bool              `json:"present"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// TemplateBuildInfo is the GET /templates/{templateID}/builds/{buildID}/status reply.
type TemplateBuildInfo struct {
	TemplateID string             `json:"templateID"`
	BuildID    string             `json:"buildID"`
	Status     string             `json:"status"`
	Logs       []string           `json:"logs"`
	LogEntries []BuildLogEntry    `json:"logEntries"`
	Reason     *BuildStatusReason `json:"reason,omitempty"`
}

// BuildLogEntry is one build log line.
type BuildLogEntry struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
	Level     string `json:"level"`
	Step      string `json:"step,omitempty"`
}

// BuildStatusReason says why a build failed and at which step.
type BuildStatusReason struct {
	Message    string          `json:"message"`
	Step       string          `json:"step,omitempty"`
	LogEntries []BuildLogEntry `json:"logEntries"`
}

// TemplateAliasResponse is the GET /templates/aliases/{alias} reply (spec: TemplateAliasResponse).
type TemplateAliasResponse struct {
	TemplateID string `json:"templateID"`
	Public     bool   `json:"public"`
}

// APIError is the e2b error envelope. The SDK surfaces `message` on failures.
type APIError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}
