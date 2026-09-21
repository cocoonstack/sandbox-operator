// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cocoonstack/sandbox-operator/internal/version"
)

const (
	LaunchTypeWarm    = "warm"
	LaunchTypeCold    = "cold"
	LaunchTypeUnknown = "unknown" // fallback for a nil Sandbox and for NormalizeCreatedBy

	// ObservabilityAnnotation is the annotation key for the time the controller first observed the claim.
	ObservabilityAnnotation = "agents.x-k8s.io/controller-first-observed-at"

	// WebhookAnnotation is the annotation key for the time the webhook first saw the claim.
	WebhookAnnotation = "agents.x-k8s.io/webhook-first-observed-at"
)

var (
	// ClaimStartupLatency measures SandboxClaim creation to Ready; sandbox_template
	// is the resolved ref, warmpool_name the requested one.
	ClaimStartupLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agent_sandbox_claim_startup_latency_ms",
			Help:    "End-to-end latency from SandboxClaim creation to Sandbox Ready state in milliseconds.",
			Buckets: []float64{100, 250, 500, 750, 1000, 1250, 1500, 2000, 2500, 5000, 10000, 30000, 60000, 120000, 240000},
		},
		[]string{"launch_type", "sandbox_template", "warmpool_name"},
	)

	// ClaimControllerStartupLatency measures controller-first-observed to Ready,
	// excluding webhook/apiserver time; labels as on ClaimStartupLatency.
	ClaimControllerStartupLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agent_sandbox_claim_controller_startup_latency_ms",
			Help:    "Latency from controller first observed SandboxClaim to Sandbox Ready state in milliseconds.",
			Buckets: []float64{100, 250, 500, 750, 1000, 1250, 1500, 2000, 2500, 5000, 10000, 30000, 60000, 120000, 240000},
		},
		[]string{"launch_type", "sandbox_template", "warmpool_name"},
	)

	// SandboxCreationLatency measures Sandbox creation to Pod Ready.
	SandboxCreationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agent_sandbox_creation_latency_ms",
			Help:    "Latency from Sandbox creation to Pod Ready state in milliseconds. For warm launches, this measures controller synchronization overhead since the Pod is pre-provisioned.",
			Buckets: []float64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 240000, 300000, 600000},
		},
		[]string{"namespace", "launch_type", "sandbox_template"},
	)

	// SandboxClaimCreationTotal counts created SandboxClaims.
	SandboxClaimCreationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_sandbox_claim_creation_total",
			Help: "Total number of SandboxClaims created, labeled by namespace, sandbox template, launch type, warmpool name, pod condition, and created_by.",
		},
		[]string{"namespace", "sandbox_template", "launch_type", "warmpool_name", "pod_condition", "created_by"},
	)

	// WarmPoolSandboxCreatedTotal counts Sandboxes created by the warm-pool controller (pool fill and
	// claim-consumed replacement alike); an event counter, so per-interval fill rates survive scrape gaps.
	WarmPoolSandboxCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_sandbox_warm_created_total",
			Help: "Total number of Sandboxes created by the SandboxWarmPool controller.",
		},
		[]string{"namespace", "warmpool_name"},
	)

	// AgentSandboxesDesc describes the point-in-time agent_sandboxes gauge.
	AgentSandboxesDesc = prometheus.NewDesc(
		"agent_sandboxes",
		"Monitor the point-in-time number of sandboxes in the cluster.",
		[]string{"namespace", "ready_condition", "expired", "launch_type", "sandbox_template", "owned_by", "created_by"},
		nil,
	)

	buildVersionInfo = version.Get()

	// BuildInfo exposes sandbox-operator build metadata as a constant gauge.
	BuildInfo = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "agent_sandbox_build_info",
			Help: "Agent sandbox controller build metadata exposed as labels with a constant value of 1.",
			ConstLabels: prometheus.Labels{
				"git_version": buildVersionInfo.GitVersion,
				"git_commit":  buildVersionInfo.GitSHA,
				"build_date":  buildVersionInfo.BuildDate,
				"go_version":  buildVersionInfo.GoVersion,
				"compiler":    buildVersionInfo.Compiler,
				"platform":    buildVersionInfo.Platform,
			},
		},
		func() float64 { return 1 },
	)
)

// Register adds the operator's collectors to r. A binary calls it once.
func Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{
		ClaimStartupLatency,
		ClaimControllerStartupLatency,
		SandboxCreationLatency,
		SandboxClaimCreationTotal,
		WarmPoolSandboxCreatedTotal,
		BuildInfo,
	} {
		if err := r.Register(c); err != nil {
			return fmt.Errorf("register operator metrics: %w", err)
		}
	}
	return nil
}

// IncWarmPoolSandboxCreated counts one Sandbox created by the warm-pool controller.
func IncWarmPoolSandboxCreated(namespace, warmPoolName string) {
	WarmPoolSandboxCreatedTotal.WithLabelValues(namespace, warmPoolName).Inc()
}

// RecordClaimStartupLatency records the duration since the provided start time.
func RecordClaimStartupLatency(startTime time.Time, launchType, templateName, warmPoolName string) {
	duration := float64(time.Since(startTime).Milliseconds())
	ClaimStartupLatency.WithLabelValues(launchType, templateName, warmPoolName).Observe(duration)
}

// RecordClaimControllerStartupLatency records the duration since the provided controller start time.
func RecordClaimControllerStartupLatency(startTime time.Time, launchType, templateName, warmPoolName string) {
	duration := float64(time.Since(startTime).Milliseconds())
	ClaimControllerStartupLatency.WithLabelValues(launchType, templateName, warmPoolName).Observe(duration)
}

// RecordSandboxCreationLatency records the measured latency duration for a sandbox creation.
func RecordSandboxCreationLatency(duration time.Duration, namespace, launchType, templateName string) {
	SandboxCreationLatency.WithLabelValues(namespace, launchType, templateName).Observe(float64(duration.Milliseconds()))
}

// NormalizeCreatedBy maps a createdBy label outside the allow-list to "unknown".
func NormalizeCreatedBy(createdBy string) string {
	switch createdBy {
	case "go-client", "python-client", "controller", "loadgen":
		return createdBy
	default:
		return LaunchTypeUnknown
	}
}

// RecordSandboxClaimCreation increments the created-claims total; createdBy is normalized automatically.
func RecordSandboxClaimCreation(namespace, templateName, launchType, warmPoolName, podCondition, createdBy string) {
	SandboxClaimCreationTotal.WithLabelValues(namespace, templateName, launchType, warmPoolName, podCondition, NormalizeCreatedBy(createdBy)).Inc()
}
