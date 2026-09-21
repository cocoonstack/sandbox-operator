// Copyright 2025 The Kubernetes Authors.
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

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
)

const (
	sandboxTemplateRefHash          = sandboxv1beta1.SandboxTemplateRefHashLabel
	warmPoolSandboxLabel            = sandboxv1beta1.SandboxWarmPoolLabel
	sandboxCreateDeleteMaxBatchSize = 300
	warmPoolEvictionAnnotation      = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	// sandboxWarmPoolLabelIndex is the cache field index over the warmPoolSandboxLabel
	// value on warm sandboxes, so reconcilePool's member lookup is O(pool members) instead
	// of O(sandboxes-in-namespace).
	sandboxWarmPoolLabelIndex = ".metadata.labels[" + warmPoolSandboxLabel + "]"
)

// staleCheck is the resolved template a pool member is vetted against, with the memo of hashes already compared.
type staleCheck struct {
	template      *extensionsv1beta1.SandboxTemplate
	refHash       string
	blueprintHash string
	vetted        map[string]bool
}

// SandboxWarmPoolReconciler reconciles a SandboxWarmPool object.
type SandboxWarmPoolReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	MaxBatchSize           int
	EnableWarmPoolEviction bool
	// DisableSandboxCRManagement makes the controller stop creating/deleting
	// per-pool Sandbox CRs and only report status. In the L3 writable-aggregation
	// design warm capacity is driven per node by sandboxd (PUT /v1/pools), NOT by
	// N Sandbox CRs — and because Sandbox create now routes through the aggregated
	// apiserver's node-local claim, CR-based replenishment would fight that path
	// (claiming the very warm VMs the pool is meant to hold). The zero value keeps
	// the legacy CR-management behavior, so existing deployments are unaffected.
	DisableSandboxCRManagement bool
	// Tracer stamps the creating trace onto each pool Sandbox so the Sandbox
	// controller does not have to back-patch it on first reconcile — that patch
	// costs one apiserver write per Sandbox, i.e. a full batch per pool refill.
	Tracer asmetrics.Instrumenter
}

//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxwarmpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxwarmpools/finalizers,verbs=get;update;patch
//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxwarmpools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch;create;update;patch;delete

func (r *SandboxWarmPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	warmPool := &extensionsv1beta1.SandboxWarmPool{}
	if err := r.Get(ctx, req.NamespacedName, warmPool); err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("SandboxWarmPool resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get SandboxWarmPool")
		return ctrl.Result{}, err
	}

	if !warmPool.DeletionTimestamp.IsZero() {
		logger.Info("SandboxWarmPool is being deleted")
		return ctrl.Result{}, nil
	}

	oldStatus := warmPool.Status.DeepCopy()

	requeueAfter, err := r.reconcilePool(ctx, warmPool)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, oldStatus, warmPool); err != nil {
		logger.Error(err, "Failed to update SandboxWarmPool status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SandboxWarmPoolReconciler) SetupWithManager(mgr ctrl.Manager, concurrentWorkers int) error {
	if r.MaxBatchSize <= 0 {
		r.MaxBatchSize = sandboxCreateDeleteMaxBatchSize
	}

	ctx := context.Background()
	if err := mgr.GetFieldIndexer().IndexField(ctx, &sandboxv1beta1.Sandbox{},
		sandboxWarmPoolLabelIndex, sandboxWarmPoolLabelIndexer); err != nil {
		return fmt.Errorf("failed to index sandboxes by warm pool label: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &extensionsv1beta1.SandboxWarmPool{},
		extensionsv1beta1.TemplateRefField, sandboxTemplateRefNameIndexer); err != nil {
		return fmt.Errorf("failed to index warm pools by template reference name: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&extensionsv1beta1.SandboxWarmPool{}).
		Owns(&sandboxv1beta1.Sandbox{}, builder.WithPredicates(predicate.Or(predicate.LabelChangedPredicate{}, poolMemberChangePredicate()))).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentWorkers}).
		Watches(
			&extensionsv1beta1.SandboxTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.findWarmPoolsForTemplate),
		).
		Complete(r)
}

// reconcilePool ensures the correct number of pre-allocated sandboxes exist in the pool.
func (r *SandboxWarmPoolReconciler) reconcilePool(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool) (time.Duration, error) {
	logger := log.FromContext(ctx)

	if r.DisableSandboxCRManagement {
		return 0, r.reconcilePoolStatusOnly(ctx, warmPool)
	}

	poolNameHash := hash.Name(warmPool.Name)

	sandboxList := &sandboxv1beta1.SandboxList{}
	labelSelector := labels.SelectorFromSet(labels.Set{
		warmPoolSandboxLabel: poolNameHash,
	})

	// Copy-free cache read: items are value snapshots sharing cache-owned maps
	// and slices, so every member mutation below deep-copies its target first.
	if err := r.List(ctx, sandboxList,
		client.InNamespace(warmPool.Namespace),
		client.MatchingFields{sandboxWarmPoolLabelIndex: poolNameHash},
		client.UnsafeDisableDeepCopy,
	); err != nil {
		logger.Error(err, "Failed to list sandboxes")
		return 0, err
	}

	template, currentSandboxBlueprintHash, tmplErr := r.fetchTemplateAndHash(ctx, warmPool)

	activeSandboxes, allErrors := r.filterActiveSandboxes(ctx, warmPool, sandboxList.Items, template, currentSandboxBlueprintHash, tmplErr)

	const warmPoolReadinessGracePeriod = 5 * time.Minute

	now := time.Now()
	var healthySandboxes []*sandboxv1beta1.Sandbox
	var stuckRecheck time.Duration
	for _, sb := range activeSandboxes {
		if !isSandboxReady(sb) && !sb.CreationTimestamp.IsZero() {
			age := now.Sub(sb.CreationTimestamp.Time)
			if age > warmPoolReadinessGracePeriod {
				logger.Info("Deleting stuck warm pool sandbox",
					"sandbox", sb.Name,
					"age", age.Round(time.Second))
				if err := r.Delete(ctx, sb); err != nil {
					logger.Error(err, "Failed to delete stuck sandbox", "sandbox", sb.Name)
					allErrors = errors.Join(allErrors, err)
				}
				continue
			}
			// Member events are change-filtered, so the grace deadline needs its
			// own timer instead of riding on unrelated pool churn.
			stuckRecheck = soonerRequeue(stuckRecheck, warmPoolReadinessGracePeriod-age)
		}
		healthySandboxes = append(healthySandboxes, sb)
	}
	activeSandboxes = healthySandboxes

	desiredReplicas := int32(1)
	if warmPool.Spec.Replicas != nil {
		desiredReplicas = *warmPool.Spec.Replicas
	}
	currentReplicas := int32(len(activeSandboxes))

	logger.Info("Pool status",
		"desired", desiredReplicas,
		"current", currentReplicas,
		"poolName", warmPool.Name,
		"poolNameHash", poolNameHash)

	warmPool.Status.Replicas = currentReplicas
	warmPool.Status.Selector = labelSelector.String()

	readyReplicas := int32(0)
	for i := range activeSandboxes {
		if isSandboxReady(activeSandboxes[i]) {
			readyReplicas++
		}
	}
	warmPool.Status.ReadyReplicas = readyReplicas

	maxBatchSize := int32(r.MaxBatchSize)

	if currentReplicas < desiredReplicas && tmplErr == nil {
		sandboxesToCreate := min(desiredReplicas-currentReplicas, maxBatchSize)
		logger.Info("Creating new pool sandboxes", "count", sandboxesToCreate)

		sandboxCR, err := r.buildSandboxCR(ctx, warmPool, template, currentSandboxBlueprintHash)
		if err != nil {
			logger.Error(err, "Failed to build sandbox CR blueprint")
			allErrors = errors.Join(allErrors, err)
		} else {
			_, createErr := slowStartBatch(ctx, int(sandboxesToCreate), 1, func(_ int) error {
				return r.createPoolSandbox(ctx, warmPool, sandboxCR)
			})
			if createErr != nil {
				logger.Error(createErr, "Failed to create pool sandboxes")
				allErrors = errors.Join(allErrors, createErr)
			}
		}
	}

	if currentReplicas > desiredReplicas {
		sandboxesToDelete := min(currentReplicas-desiredReplicas, maxBatchSize)
		logger.Info("Deleting excess sandboxes", "count", sandboxesToDelete)

		// Prioritize deleting unready sandboxes before ready ones,
		// then newest first within each group.
		slices.SortFunc(activeSandboxes, func(a, b *sandboxv1beta1.Sandbox) int {
			aReady := isSandboxReady(a)
			bReady := isSandboxReady(b)
			if aReady != bReady {
				if aReady {
					return 1
				}
				return -1
			}
			return b.CreationTimestamp.Compare(a.CreationTimestamp.Time)
		})

		toDeleteCount := min(sandboxesToDelete, int32(len(activeSandboxes)))
		_, deleteErr := slowStartBatch(ctx, int(toDeleteCount), 1, func(idx int) error {
			return r.deletePoolSandbox(ctx, activeSandboxes[idx])
		})
		if deleteErr != nil {
			logger.Error(deleteErr, "Failed to delete pool sandboxes")
			allErrors = errors.Join(allErrors, deleteErr)
		}
	}

	if tmplErr != nil && !k8serrors.IsNotFound(tmplErr) {
		allErrors = errors.Join(allErrors, tmplErr)
	}

	return stuckRecheck, allErrors
}

func (r *SandboxWarmPoolReconciler) reconcilePoolStatusOnly(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool) error {
	poolNameHash := hash.Name(warmPool.Name)
	labelSelector := labels.SelectorFromSet(labels.Set{warmPoolSandboxLabel: poolNameHash})

	sandboxList := &sandboxv1beta1.SandboxList{}
	// Copy-free cache read; this branch only counts, never mutates members.
	if err := r.List(ctx, sandboxList,
		client.InNamespace(warmPool.Namespace),
		client.MatchingFields{sandboxWarmPoolLabelIndex: poolNameHash},
		client.UnsafeDisableDeepCopy,
	); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list sandboxes (status-only reconcile)")
		return err
	}

	readyReplicas := int32(0)
	for i := range sandboxList.Items {
		if isSandboxReady(&sandboxList.Items[i]) {
			readyReplicas++
		}
	}
	warmPool.Status.Replicas = int32(len(sandboxList.Items))
	warmPool.Status.ReadyReplicas = readyReplicas
	warmPool.Status.Selector = labelSelector.String()
	return nil
}

// adoptSandbox sets this warmpool as the owner of an orphaned sandbox.
func (r *SandboxWarmPoolReconciler) adoptSandbox(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool, sb *sandboxv1beta1.Sandbox) error {
	if err := controllerutil.SetControllerReference(warmPool, sb, r.Scheme); err != nil {
		return err
	}
	setWarmLaunchTypeLabel(sb)
	return r.Update(ctx, sb)
}

// filterActiveSandboxes filters the list of sandboxes, deleting stale ones and adopting orphans.
func (r *SandboxWarmPoolReconciler) filterActiveSandboxes(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool, sandboxes []sandboxv1beta1.Sandbox, template *extensionsv1beta1.SandboxTemplate, currentSandboxBlueprintHash string, tmplErr error) ([]*sandboxv1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	var activeSandboxes []*sandboxv1beta1.Sandbox
	var allErrors error

	check := staleCheck{template: template, blueprintHash: currentSandboxBlueprintHash, vetted: make(map[string]bool)}
	if template != nil {
		check.refHash = SandboxTemplateRefHash(template.Name)
	}

	var updateStrategyType extensionsv1beta1.SandboxWarmPoolUpdateStrategyType
	if warmPool.Spec.UpdateStrategy != nil {
		updateStrategyType = warmPool.Spec.UpdateStrategy.Type
	}

	recreate := updateStrategyType == extensionsv1beta1.RecreateSandboxWarmPoolUpdateStrategyType
	if !recreate && updateStrategyType != "" &&
		updateStrategyType != extensionsv1beta1.OnReplenishSandboxWarmPoolUpdateStrategyType {
		logger.Info("Unknown update strategy, defaulting to OnReplenish", "strategy", updateStrategyType)
	}

	for i := range sandboxes {
		sb := &sandboxes[i]
		if !sb.DeletionTimestamp.IsZero() {
			continue
		}

		controllerRef := metav1.GetControllerOf(sb)
		isOrphan := controllerRef == nil
		isControlledByPool := controllerRef != nil && controllerRef.UID == warmPool.UID

		if !isOrphan && !isControlledByPool {
			logger.Info("Ignoring sandbox with different controller", "sandbox", sb.Name, "controller", controllerRef.Name)
			continue
		}

		if tmplErr == nil && (recreate || isOrphan) {
			if r.isSandboxStale(ctx, sb, check) {
				logger.Info("Deleting stale sandbox", "sandbox", sb.Name, "isOrphan", isOrphan)
				if err := r.Delete(ctx, sb); err != nil {
					logger.Error(err, "Failed to delete stale sandbox", "sandbox", sb.Name)
					allErrors = errors.Join(allErrors, err)
				}
				continue
			}
		}

		// sb shares cache-owned maps (copy-free List); mutate deep copies only.
		if isControlledByPool && sb.Labels[sandboxv1beta1.SandboxLaunchTypeLabel] != sandboxv1beta1.SandboxLaunchTypeWarm {
			fresh := sb.DeepCopy()
			setWarmLaunchTypeLabel(fresh)
			if err := r.Update(ctx, fresh); err != nil {
				logger.Error(err, "Failed to update sandbox launch type label", "sandbox", sb.Name)
				allErrors = errors.Join(allErrors, err)
				continue
			}
			sb = fresh
		}

		if isOrphan {
			logger.Info("Adopting orphaned sandbox", "sandbox", sb.Name)
			fresh := sb.DeepCopy()
			if err := r.adoptSandbox(ctx, warmPool, fresh); err != nil {
				logger.Error(err, "Failed to adopt sandbox", "sandbox", sb.Name)
				allErrors = errors.Join(allErrors, err)
				continue
			}
			sb = fresh
		}

		activeSandboxes = append(activeSandboxes, sb)
	}
	return activeSandboxes, allErrors
}

// fetchTemplateAndHash fetches the sandbox template and computes its hash.
func (r *SandboxWarmPoolReconciler) fetchTemplateAndHash(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool) (*extensionsv1beta1.SandboxTemplate, string, error) {
	logger := log.FromContext(ctx)
	template, tmplErr := r.getTemplate(ctx, warmPool)
	var currentSandboxBlueprintHash string
	if tmplErr == nil {
		currentSandboxBlueprintHash, tmplErr = computeSandboxBlueprintHash(template)
	}

	if tmplErr != nil {
		logger.Error(tmplErr, "Failed to get sandbox template and hash", "templateRef", warmPool.Spec.TemplateRef.Name)
	}
	return template, currentSandboxBlueprintHash, tmplErr
}

// buildSandboxCR constructs the base Sandbox CR (with pod template and volume claim templates) for the warm pool.
func (r *SandboxWarmPoolReconciler) buildSandboxCR(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool, template *extensionsv1beta1.SandboxTemplate, currentSandboxBlueprintHash string) (*sandboxv1beta1.Sandbox, error) {
	poolNameHash := hash.Name(warmPool.Name)
	sandboxLabels := map[string]string{
		warmPoolSandboxLabel:                    poolNameHash,
		sandboxTemplateRefHash:                  SandboxTemplateRefHash(warmPool.Spec.TemplateRef.Name),
		sandboxv1beta1.SandboxLaunchTypeLabel:   sandboxv1beta1.SandboxLaunchTypeWarm,
		sandboxv1beta1.SandboxTemplateHashLabel: currentSandboxBlueprintHash,
		sandboxv1beta1.CreatedByLabel:           "controller",
	}

	sandboxAnnotations := map[string]string{
		sandboxv1beta1.SandboxTemplateRefAnnotation: warmPool.Spec.TemplateRef.Name,
	}
	if r.Tracer != nil {
		if tc := r.Tracer.GetTraceContext(ctx); tc != "" {
			sandboxAnnotations[asmetrics.TraceContextAnnotation] = tc
		}
	}

	sandbox := &sandboxv1beta1.Sandbox{
		GenerateName: fmt.Sprintf("%s-", warmPool.Name),
		Namespace:    warmPool.Namespace,
		Labels:       sandboxLabels,
		Annotations:  sandboxAnnotations,
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: *template.Spec.SandboxBlueprint.DeepCopy(),
		},
	}

	if sandbox.Spec.PodTemplate.ObjectMeta.Labels == nil {
		sandbox.Spec.PodTemplate.ObjectMeta.Labels = make(map[string]string)
	}
	sandbox.Spec.PodTemplate.ObjectMeta.Labels[warmPoolSandboxLabel] = poolNameHash
	sandbox.Spec.PodTemplate.ObjectMeta.Labels[sandboxTemplateRefHash] = SandboxTemplateRefHash(warmPool.Spec.TemplateRef.Name)
	sandbox.Spec.PodTemplate.ObjectMeta.Labels[sandboxv1beta1.SandboxTemplateHashLabel] = currentSandboxBlueprintHash

	// Respect the template's custom eviction annotation if explicitly specified.
	// Only apply the default eviction behavior if the annotation is not defined.
	if _, exists := sandbox.Spec.PodTemplate.ObjectMeta.Annotations[warmPoolEvictionAnnotation]; !exists {
		if r.EnableWarmPoolEviction {
			if sandbox.Spec.PodTemplate.ObjectMeta.Annotations == nil {
				sandbox.Spec.PodTemplate.ObjectMeta.Annotations = make(map[string]string)
			}
			sandbox.Spec.PodTemplate.ObjectMeta.Annotations[warmPoolEvictionAnnotation] = "true"
		}
	}

	ApplySandboxSecureDefaults(template, &sandbox.Spec.PodTemplate.Spec)

	if err := ctrl.SetControllerReference(warmPool, sandbox, r.Scheme); err != nil {
		return nil, fmt.Errorf("SetControllerReference for Sandbox failed: %w", err)
	}

	return sandbox, nil
}

// createPoolSandbox creates a full Sandbox CR for the warm pool using a pre-built sandboxCR.
func (r *SandboxWarmPoolReconciler) createPoolSandbox(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool, sandboxCR *sandboxv1beta1.Sandbox) error {
	logger := log.FromContext(ctx)
	sandbox := sandboxCR.DeepCopy()
	if err := r.Create(ctx, sandbox); err != nil {
		logger.Error(err, "Failed to create pool sandbox")
		return err
	}
	asmetrics.IncWarmPoolSandboxCreated(warmPool.Namespace, warmPool.Name)

	logger.Info("Created new pool sandbox", "sandbox", sandbox.Name, "poolName", warmPool.Name)
	return nil
}

// deletePoolSandbox deletes a Sandbox CR from the warm pool. Ignores not found errors to not abort the batch deletion if some sandboxes are already deleted.
func (r *SandboxWarmPoolReconciler) deletePoolSandbox(ctx context.Context, sb *sandboxv1beta1.Sandbox) error {
	logger := log.FromContext(ctx)
	if err := r.Delete(ctx, sb); err != nil && client.IgnoreNotFound(err) != nil {
		logger.Error(err, "Failed to delete sandbox", "sandbox", sb.Name, "namespace", sb.Namespace)
		return err
	}
	return nil
}

// updateStatus updates the status of the SandboxWarmPool if it has changed.
func (r *SandboxWarmPoolReconciler) updateStatus(ctx context.Context, oldStatus *extensionsv1beta1.SandboxWarmPoolStatus, warmPool *extensionsv1beta1.SandboxWarmPool) error {
	logger := log.FromContext(ctx)

	if equality.Semantic.DeepEqual(oldStatus, &warmPool.Status) {
		return nil
	}

	oldWarmPool := warmPool.DeepCopy()
	oldWarmPool.Status = *oldStatus
	patch := client.MergeFrom(oldWarmPool)

	if err := r.Status().Patch(ctx, warmPool, patch); err != nil {
		return fmt.Errorf("failed to update SandboxWarmPool status: %w", err)
	}

	logger.Info("Updated SandboxWarmPool status", "replicas", warmPool.Status.Replicas, "readyReplicas", warmPool.Status.ReadyReplicas)
	return nil
}

func (r *SandboxWarmPoolReconciler) getTemplate(ctx context.Context, warmPool *extensionsv1beta1.SandboxWarmPool) (*extensionsv1beta1.SandboxTemplate, error) {
	template := &extensionsv1beta1.SandboxTemplate{
		Namespace: warmPool.Namespace,
		Name:      warmPool.Spec.TemplateRef.Name,
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(template), template); err != nil {
		if !k8serrors.IsNotFound(err) {
			err = fmt.Errorf("failed to get sandbox template %q: %w", warmPool.Spec.TemplateRef.Name, err)
		}
		return nil, err
	}

	return template, nil
}

// isSandboxStale checks if the sandbox version matches the current template.
// It uses a cache (vettedHashes) to avoid repeated expensive DeepEqual calls
// for sandboxes with the same hash.
func (r *SandboxWarmPoolReconciler) isSandboxStale(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, check staleCheck) bool {
	sandboxHash := sandbox.Labels[sandboxv1beta1.SandboxTemplateHashLabel]

	if sandbox.Labels[sandboxTemplateRefHash] != check.refHash {
		return true
	}

	controllerRef := metav1.GetControllerOf(sandbox)
	isOrphan := controllerRef == nil
	if isOrphan {
		return !r.compareSandboxBlueprint(check.template, &sandbox.Spec.SandboxBlueprint)
	}

	if sandboxHash != "" && sandboxHash == check.blueprintHash {
		return false
	}

	// A marshal failure leaves the hash empty; treating that as stale would
	// mass-delete the pool.
	if check.blueprintHash == "" {
		log.FromContext(ctx).Error(nil, "blueprint hash is empty, skipping staleness check", "sandbox", sandbox.Name)
		return false
	}

	if sandboxHash != "" {
		if isStale, found := check.vetted[sandboxHash]; found {
			return isStale
		}
	}

	isStale := !r.compareSandboxBlueprint(check.template, &sandbox.Spec.SandboxBlueprint)

	if sandboxHash != "" {
		check.vetted[sandboxHash] = isStale
	}

	return isStale
}

// comparePodSpecs checks if the pod spec in the sandbox is semantically equal to the template,
// normalizing for fields that the controller populates by default.
func (r *SandboxWarmPoolReconciler) comparePodSpecs(template *extensionsv1beta1.SandboxTemplate, actualSandboxSpec *corev1.PodSpec) bool {
	expectedSpec := template.Spec.PodTemplate.Spec.DeepCopy()
	ApplySandboxSecureDefaults(template, expectedSpec)

	// Both sides carry the same defaulting, so a remaining difference is drift.
	return equality.Semantic.DeepEqual(expectedSpec, actualSandboxSpec)
}

// compareVolumeClaimTemplates checks if the volume claim templates in the sandbox are equal to the template.
// Only each entry's name and spec are compared, as changes in metadata (like labels, annotations) are not tracked for staleness.
// Note: Comparison is index-based (order-sensitive) to stay consistent with computeSandboxBlueprintHash (+listType=atomic).
// Making this comparison order-independent without also sorting the templates in computeSandboxBlueprintHash
// would cause reordered warm sandboxes to fail the hash label check on every reconcile.
func (r *SandboxWarmPoolReconciler) compareVolumeClaimTemplates(template *extensionsv1beta1.SandboxTemplate, actualVCTs []sandboxv1beta1.PersistentVolumeClaimTemplate) bool {
	if len(template.Spec.VolumeClaimTemplates) != len(actualVCTs) {
		return false
	}

	for i, tmplVCT := range template.Spec.VolumeClaimTemplates {
		actualVCT := actualVCTs[i]
		if tmplVCT.Name != actualVCT.Name || !equality.Semantic.DeepEqual(tmplVCT.Spec, actualVCT.Spec) {
			return false
		}
	}

	return true
}

// compareSandboxBlueprint checks if the sandbox blueprint in the sandbox is semantically equal to the template,
// ignoring metadata differences and only comparing the fields that are relevant for staleness detection.
func (r *SandboxWarmPoolReconciler) compareSandboxBlueprint(template *extensionsv1beta1.SandboxTemplate, actualSandboxSpec *sandboxv1beta1.SandboxBlueprint) bool {
	return r.comparePodSpecs(template, &actualSandboxSpec.PodTemplate.Spec) &&
		r.compareVolumeClaimTemplates(template, actualSandboxSpec.VolumeClaimTemplates) &&
		equality.Semantic.DeepEqual(template.Spec.Service, actualSandboxSpec.Service)
}

// findWarmPoolsForTemplate returns a list of reconcile.Requests for all SandboxWarmPools that reference the template.
func (r *SandboxWarmPoolReconciler) findWarmPoolsForTemplate(ctx context.Context, obj client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)
	template, ok := obj.(*extensionsv1beta1.SandboxTemplate)
	if !ok {
		return nil
	}

	warmPools := &extensionsv1beta1.SandboxWarmPoolList{}
	if err := r.List(ctx, warmPools, client.InNamespace(template.Namespace), client.MatchingFields{extensionsv1beta1.TemplateRefField: template.Name}); err != nil {
		logger.Error(err, "Failed to list warm pools for template", "template", template.Name)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(warmPools.Items))
	for _, wp := range warmPools.Items {
		requests = append(requests, reconcile.Request{
			Name:      wp.Name,
			Namespace: wp.Namespace,
		})
	}
	return requests
}

// slowStartBatch is a helper that runs a given function fn multiple times in parallel batches.
// It starts with initialBatchSize, and doubles the batch size for each successful batch.
// If any execution of fn returns an error, it stops and returns the first encountered error.
func slowStartBatch(ctx context.Context, count int, initialBatchSize int, fn func(int) error) (int, error) {
	remaining := count
	successes := 0

	for batchSize := min(remaining, initialBatchSize); batchSize > 0; batchSize = min(2*batchSize, remaining) {
		if ctx.Err() != nil {
			return successes, ctx.Err()
		}

		var eg errgroup.Group
		var batchSuccesses atomic.Int64

		for i := range batchSize {
			index := successes + i
			eg.Go(func() error {
				if err := fn(index); err != nil {
					return err
				}
				batchSuccesses.Add(1)
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			successes += int(batchSuccesses.Load())
			return successes, err
		}

		successes += int(batchSuccesses.Load())
		remaining -= batchSize
	}

	return successes, nil
}

func setWarmLaunchTypeLabel(sb *sandboxv1beta1.Sandbox) {
	if sb.Labels == nil {
		sb.Labels = make(map[string]string)
	}
	sb.Labels[sandboxv1beta1.SandboxLaunchTypeLabel] = sandboxv1beta1.SandboxLaunchTypeWarm
}

// computeSandboxBlueprintHash computes a hash of the sandbox template's Spec.SandboxBlueprint.
func computeSandboxBlueprintHash(template *extensionsv1beta1.SandboxTemplate) (string, error) {
	specJSON, err := json.Marshal(template.Spec.SandboxBlueprint)
	if err != nil {
		return "", fmt.Errorf("failed to marshal sandbox blueprint for hashing: %w", err)
	}
	return hash.Name(string(specJSON)), nil
}

// sandboxWarmPoolLabelIndexer extracts the warmPoolSandboxLabel value for the
// sandboxWarmPoolLabelIndex cache field index. Shared with tests so fake clients
// register the same index the manager does.
func sandboxWarmPoolLabelIndexer(obj client.Object) []string {
	if v, ok := obj.GetLabels()[warmPoolSandboxLabel]; ok {
		return []string{v}
	}
	return nil
}

// sandboxTemplateRefNameIndexer extracts the template reference name for the
// TemplateRefField cache field index. Shared with tests so fake clients
// register the same index the manager does.
func sandboxTemplateRefNameIndexer(obj client.Object) []string {
	wp := obj.(*extensionsv1beta1.SandboxWarmPool)
	if wp.Spec.TemplateRef.Name == "" {
		return nil
	}
	return []string{wp.Spec.TemplateRef.Name}
}

// poolMemberChangePredicate passes the non-label member transitions
// reconcilePool reads — ownership, deletion, Ready flips, and generation bumps
// (orphan blueprint re-vetting); LabelChangedPredicate joins it at registration.
// Anything else (PodIPs, other conditions) would re-scan the pool for nothing.
func poolMemberChangePredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSb, okOld := e.ObjectOld.(*sandboxv1beta1.Sandbox)
			newSb, okNew := e.ObjectNew.(*sandboxv1beta1.Sandbox)
			if !okOld || !okNew {
				return true
			}
			// The reconcile reads only the controller ref, so compare that —
			// DeepEqual over the owner slice costs ~200x per member event.
			oldRef, newRef := metav1.GetControllerOf(oldSb), metav1.GetControllerOf(newSb)
			return oldSb.Generation != newSb.Generation ||
				oldSb.DeletionTimestamp.IsZero() != newSb.DeletionTimestamp.IsZero() ||
				(oldRef == nil) != (newRef == nil) ||
				(oldRef != nil && oldRef.UID != newRef.UID) ||
				isSandboxReady(oldSb) != isSandboxReady(newSb)
		},
	}
}
