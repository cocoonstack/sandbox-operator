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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
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

	v1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/lifecycle"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
)

const (
	reasonTemplateNotFound         = "TemplateNotFound"
	reasonInvalidMetadata          = "InvalidMetadata"
	reasonEnvVarsInjectionRejected = "EnvVarsInjectionRejected"
	reasonReconcilerError          = "ReconcilerError"
	reasonSandboxNotReady          = "SandboxNotReady"
	msgSandboxNotReady             = "Sandbox is not ready"

	claimKind    = "SandboxClaim"
	warmPoolKind = "SandboxWarmPool"

	immediateRequeueDelay = time.Millisecond
	// adoptionCacheLagRequeueDelay sets the latency floor for a warm-pool claim: the claim is only marked Bound on the pass that observes the adopted Sandbox.
	adoptionCacheLagRequeueDelay = 5 * time.Millisecond
)

var (
	ErrTemplateNotFound                      = errors.New("SandboxTemplate not found")
	ErrInvalidMetadata                       = errors.New("invalid additionalPodMetadata")
	ErrSandboxNotOwned                       = errors.New("sandbox not owned by this claim")
	ErrWarmPoolNotFound                      = errors.New("SandboxWarmPool not found")
	ErrCrossNamespaceAdoption                = errors.New("cross-namespace adoption forbidden")
	ErrEnvVarsInjectionRejected              = errors.New("environment variable injection rejected")
	ErrVolumeClaimTemplatesDisallowed        = errors.New("volume claim templates are disallowed by the template")
	ErrVolumeClaimTemplatesOverrideForbidden = errors.New("overriding volume claim templates is forbidden by the template")
	ErrVolumeClaimTemplatesInvalid           = errors.New("invalid volume claim templates")

	// errAdoptionTriggeredRetry is a sentinel so Reconcile can requeue instead of returning an error: the failure rate limiter would compound backoff on every pass until the informer cache converges (#1107).
	errAdoptionTriggeredRetry = errors.New("triggered adoption completion, retry")

	restrictedDomains = []string{"kubernetes.io", "k8s.io", "agents.x-k8s.io"}

	suppressErrors = []error{
		ErrInvalidMetadata,
		ErrSandboxNotOwned,
		ErrEnvVarsInjectionRejected,
		ErrVolumeClaimTemplatesDisallowed,
		ErrVolumeClaimTemplatesOverrideForbidden,
		ErrVolumeClaimTemplatesInvalid,
	}
)

// observedTimeEntry stores the first observed timestamp and the UID of the SandboxClaim.
// We store the UID to protect against stale data when a claim is deleted and a new one
// is created with the same name.
type observedTimeEntry struct {
	timestamp time.Time
	uid       types.UID
}

// triggeredAdoptionEntry records that completeAdoption already patched the
// named sandbox over to a claim (identified by UID), so cache-lag requeues
// can wait for the informer to converge without re-sending the patch.
type triggeredAdoptionEntry struct {
	uid     types.UID
	sandbox string
}

type nodeSpread struct {
	count int
	first int
}

// failure is the reason/message pair behind a not-Ready claim.
type failure struct {
	reason  string
	message string
}

// SandboxClaimReconciler reconciles a SandboxClaim object.
type SandboxClaimReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	WarmSandboxQueue    *queue.SimpleSandboxQueue
	Recorder            events.EventRecorder
	Tracer              asmetrics.Instrumenter
	observedTimes       syncMap[types.NamespacedName, observedTimeEntry]
	triggeredAdoptions  syncMap[types.NamespacedName, triggeredAdoptionEntry]
	AllowedLabelDomains []string
}

//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxclaims/finalizers,verbs=get;update;patch
//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxclaims/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=extensions.agents.x-k8s.io,resources=sandboxtemplates,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch;update
//+kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *SandboxClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Start of Reconcile loop for SandboxClaim", "request", req.NamespacedName)
	claim := &extensionsv1beta1.SandboxClaim{}
	if err := r.Get(ctx, req.NamespacedName, claim); err != nil {
		if k8errors.IsNotFound(err) {
			// Fallback cleanup to prevent memory leaks if the delete predicate was missed or a stale request is processed.
			r.observedTimes.Delete(req.NamespacedName)
			r.triggeredAdoptions.Delete(req.NamespacedName)
			logger.V(1).Info("SandboxClaim not found, ignoring", "request", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get sandbox claim %q: %w", req.NamespacedName, err)
	}

	var initialAttrs map[string]string
	if claim.Labels != nil {
		if val, ok := claim.Labels[v1beta1.CreatedByLabel]; ok && val != "" {
			initialAttrs = map[string]string{
				v1beta1.CreatedByLabel: asmetrics.NormalizeCreatedBy(val),
			}
		}
	}
	ctx, end := r.Tracer.StartSpan(ctx, claim, "ReconcileSandboxClaim", initialAttrs)
	defer end()

	if !claim.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Staged, not written: on the warm path the adoption Update carries these
	// annotations, so the claim costs three apiserver writes instead of four.
	flushAnnotations := r.stageAnnotations(ctx, claim)

	originalClaimStatus := claim.Status.DeepCopy()

	claimExpired, timeLeft := r.checkExpiration(claim)
	if claimExpired && !readyReasonIs(claim.Status.Conditions, extensionsv1beta1.ClaimExpiredReason) {
		// Status writes cannot carry metadata, so the staged annotations must be
		// flushed before this path returns or they are lost for good.
		if err := flushAnnotations(); err != nil {
			return ctrl.Result{}, err
		}
		meta.SetStatusCondition(&claim.Status.Conditions, r.computeReadyCondition(claim, nil, nil, true))
		if updateErr := r.updateStatus(ctx, originalClaimStatus, claim); updateErr != nil {
			logger.V(1).Info("Sandboxclaim UpdateStatus error encountered", "errors", updateErr, "request", req.NamespacedName)
			return ctrl.Result{}, updateErr
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, extensionsv1beta1.ClaimExpiredReason, "Claim Expired", "Claim expired")
		}
		return ctrl.Result{RequeueAfter: immediateRequeueDelay}, nil
	}
	logger.V(1).Info("Expiration check", "isExpired", claimExpired, "timeLeft", timeLeft, "request", req.NamespacedName)

	// Delete policies return immediately: a status update on the deleted object
	// would error.
	if claimExpired && claim.Spec.Lifecycle != nil &&
		(claim.Spec.Lifecycle.ShutdownPolicy == extensionsv1beta1.ShutdownPolicyDelete ||
			claim.Spec.Lifecycle.ShutdownPolicy == extensionsv1beta1.ShutdownPolicyDeleteForeground) {

		policy := claim.Spec.Lifecycle.ShutdownPolicy
		logger.Info("Deleting Claim because time has expired", "shutdownPolicy", policy, "claim", claim.Name)
		if r.Recorder != nil {
			r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, extensionsv1beta1.ClaimExpiredReason, "Deleting", fmt.Sprintf("Deleting Claim (ShutdownPolicy=%s)", policy))
		}

		// The claim is about to go away; persist what was observed about it first.
		if err := flushAnnotations(); err != nil {
			return ctrl.Result{}, err
		}

		deleteOpts := []client.DeleteOption{}
		if policy == extensionsv1beta1.ShutdownPolicyDeleteForeground {
			deleteOpts = append(deleteOpts, client.PropagationPolicy(metav1.DeletePropagationForeground))
		}

		if err := r.Delete(ctx, claim, deleteOpts...); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{}, nil
	}

	var sandbox *v1beta1.Sandbox
	var reconcileErr error

	if claimExpired {
		sandbox, reconcileErr = r.reconcileExpired(ctx, claim)
	} else {
		sandbox, reconcileErr = r.reconcileActive(ctx, claim)
	}

	// Every path below this point only touches status, so one flush here covers
	// the passes where no adoption write happened.
	if err := flushAnnotations(); err != nil {
		return ctrl.Result{}, errors.Join(reconcileErr, err)
	}

	r.computeAndSetStatus(claim, sandbox, reconcileErr, claimExpired)
	postExpiration, _ := r.checkExpiration(claim)
	if postExpiration && !readyReasonIs(claim.Status.Conditions, extensionsv1beta1.ClaimExpiredReason) {
		meta.SetStatusCondition(&claim.Status.Conditions, r.computeReadyCondition(claim, sandbox, reconcileErr, true))
		if updateErr := r.updateStatus(ctx, originalClaimStatus, claim); updateErr != nil {
			errs := errors.Join(reconcileErr, updateErr)
			logger.V(1).Info("Sandboxclaim UpdateStatus error encountered", "errors", errs, "request", req.NamespacedName)
			return ctrl.Result{}, errs
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, extensionsv1beta1.ClaimExpiredReason, "Claim Expired", "Claim expired")
		}
		return ctrl.Result{RequeueAfter: immediateRequeueDelay}, nil
	}

	if updateErr := r.updateStatus(ctx, originalClaimStatus, claim); updateErr != nil {
		errs := errors.Join(reconcileErr, updateErr)
		logger.V(1).Info("Sandboxclaim UpdateStatus error encountered", "errors", errs, "request", req.NamespacedName)
		return ctrl.Result{}, errs
	}

	r.recordCreationLatencyMetric(ctx, claim, originalClaimStatus, sandbox)

	result, deferred := r.resultFor(ctx, claim, reconcileErr, claimExpired)
	if deferred {
		return result, nil
	}
	logger.V(1).Info("End of Reconcile loop SandboxClaim", "result", result, "error", reconcileErr, "request", req.NamespacedName)
	return result, reconcileErr
}

// SetupWithManager sets up the controller with the Manager.
func (r *SandboxClaimReconciler) SetupWithManager(mgr ctrl.Manager, concurrentWorkers int) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &extensionsv1beta1.SandboxClaim{}, extensionsv1beta1.WarmPoolRefField, warmPoolRefIndexer); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&extensionsv1beta1.SandboxClaim{}, builder.WithPredicates(r.getTimingPredicate())).
		Owns(&v1beta1.Sandbox{}, builder.WithPredicates(claimSandboxChangePredicate())).
		Watches(&v1beta1.Sandbox{}, &sandboxEventHandler{sandboxQueue: r.WarmSandboxQueue}).
		Watches(&extensionsv1beta1.SandboxWarmPool{}, &warmPoolEventHandler{sandboxQueue: r.WarmSandboxQueue}).
		Watches(
			&extensionsv1beta1.SandboxWarmPool{},
			handler.EnqueueRequestsFromMapFunc(r.mapWarmPoolToClaims),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentWorkers}).
		Complete(r)
}

// resultFor turns the reconcile outcome into a requeue decision. The bool reports
// that the error is deliberately swallowed: a missing dependency, an adoption
// waiting on cache convergence, or a user-configuration error that must not drive
// the workqueue's exponential backoff.
func (r *SandboxClaimReconciler) resultFor(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, reconcileErr error, claimExpired bool) (ctrl.Result, bool) {
	logger := log.FromContext(ctx)
	postExpiration, postTimeLeft := r.checkExpiration(claim)

	var result ctrl.Result
	if !claimExpired {
		switch {
		case postExpiration:
			result = ctrl.Result{RequeueAfter: immediateRequeueDelay}
		case postTimeLeft > 0:
			result = ctrl.Result{RequeueAfter: postTimeLeft}
		}
	}

	// A dependency that has not been created yet is normal during rollout, not a
	// failure to back off from.
	// TODO: This 1-minute requeue creates a latency regression vs an immediate watch
	// trigger. Consider a lightweight SandboxTemplate -> claims map watch.
	if errors.Is(reconcileErr, ErrWarmPoolNotFound) || errors.Is(reconcileErr, ErrTemplateNotFound) {
		logger.V(1).Info("Dependency not found yet, will retry", "warmPool", claim.Spec.WarmPoolRef.Name, "error", reconcileErr)
		return ctrl.Result{RequeueAfter: soonerRequeue(result.RequeueAfter, time.Minute)}, true
	}

	if errors.Is(reconcileErr, errAdoptionTriggeredRetry) {
		logger.V(4).Info("Adoption triggered; requeueing to let cache converge", "claim", claim.Name, "error", reconcileErr)
		return ctrl.Result{RequeueAfter: soonerRequeue(result.RequeueAfter, adoptionCacheLagRequeueDelay)}, true
	}

	if shouldSuppressError(reconcileErr) {
		logger.V(1).Info("Sandboxclaim suppressed error(s) encountered", "error", reconcileErr, "claim", claim.Name)
		return result, true
	}
	return result, false
}

// stageAnnotations records the observability and trace annotations on claim in
// memory and returns the writer that persists them. Adoption writes the whole
// claim, so on the warm path its Update carries them and the writer costs
// nothing; every other pass pays one metadata patch, as before. Every return in
// Reconcile after this point runs the writer first.
func (r *SandboxClaimReconciler) stageAnnotations(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) func() error {
	traceContext := r.Tracer.GetTraceContext(ctx)
	needObservability := claim.Annotations[asmetrics.ObservabilityAnnotation] == ""
	needTraceContext := traceContext != "" && claim.Annotations[asmetrics.TraceContextAnnotation] == ""
	if !needObservability && !needTraceContext {
		return func() error { return nil }
	}

	before := claim.DeepCopy()
	if claim.Annotations == nil {
		claim.Annotations = make(map[string]string)
	}
	// Only the keys staged here, so a replay says exactly what it means. Replaying
	// the whole map happens to be safe — every other key is also in `before`, so
	// the merge patch for it is empty — but it reads like it could undo a clear.
	staged := map[string]string{}
	if needObservability {
		staged[asmetrics.ObservabilityAnnotation] = r.getOrRecordObservedTime(claim).Format(time.RFC3339Nano)
	}
	if needTraceContext {
		staged[asmetrics.TraceContextAnnotation] = traceContext
	}
	maps.Copy(claim.Annotations, staged)
	startRV := claim.ResourceVersion
	return func() error {
		// Every write decodes the server's copy back into claim, so a partial
		// patch by another step (legacy-label migration, stale-reference clearing)
		// strips the staged values even though it advanced the resourceVersion.
		// A newer object that still carries every staged value is what matters —
		// whichever write put them there, they are on the server.
		if claim.ResourceVersion != startRV && stagedAnnotationsSurvived(claim, staged) {
			return nil
		}
		// A partial patch may have stripped them from the decoded object; put them
		// back on claim itself so the caller keeps the server's response.
		if claim.Annotations == nil {
			claim.Annotations = make(map[string]string)
		}
		maps.Copy(claim.Annotations, staged)
		return r.Patch(ctx, claim, client.MergeFrom(before))
	}
}

// syncAdoptedSandboxMetadata brings a found or adopted Sandbox's pod template in
// line with the claim. The template is best-effort: without it there is nothing to
// merge, unless the claim carries metadata that must be propagated.
func (r *SandboxClaimReconciler) syncAdoptedSandboxMetadata(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, sandbox *v1beta1.Sandbox) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Fast path: sandbox found or adopted, reconciling network policy", "claim", claim.Name)

	template, templateErr := r.getTemplate(ctx, claim)
	if templateErr != nil {
		logger.Error(templateErr, "failed to get template of the warmpool for network policy reconciliation (non-fatal)", "claim", claim.Name, "warmPool", claim.Spec.WarmPoolRef.Name)
		// Metadata to propagate with no template to merge it into would silently
		// break the "no overrides" rule, so fail instead.
		if len(claim.Spec.AdditionalPodMetadata.Labels) > 0 || len(claim.Spec.AdditionalPodMetadata.Annotations) > 0 {
			return fmt.Errorf("failed to get template for metadata propagation: %w", templateErr)
		}
	}
	if template == nil {
		return nil
	}

	var mergedMeta v1beta1.PodMetadata
	template.Spec.PodTemplate.ObjectMeta.DeepCopyInto(&mergedMeta)
	if mergedMeta.Labels == nil {
		mergedMeta.Labels = make(map[string]string)
	}
	templateHash := SandboxTemplateRefHash(template.Name)
	createdBy := claim.Labels[v1beta1.CreatedByLabel]
	mergedMeta.Labels[extensionsv1beta1.SandboxIDLabel] = string(claim.UID)
	mergedMeta.Labels[sandboxTemplateRefHash] = templateHash
	// A claim without the label must clear it, so a warm sandbox never keeps a
	// stale created-by from whoever held it before.
	setOrDeleteLabel(mergedMeta.Labels, v1beta1.CreatedByLabel, createdBy)

	if err := r.mergePodMetadata(&mergedMeta, &claim.Spec.AdditionalPodMetadata); err != nil {
		return err
	}

	// Compared before any mutation so the steady state costs no deep copy.
	needsUpdate := !equality.Semantic.DeepEqual(&mergedMeta, &sandbox.Spec.PodTemplate.ObjectMeta) ||
		sandbox.Labels[sandboxTemplateRefHash] != templateHash ||
		sandbox.Labels[v1beta1.CreatedByLabel] != createdBy
	if !needsUpdate {
		return nil
	}

	patch := client.MergeFrom(sandbox.DeepCopy())
	if sandbox.Labels == nil {
		sandbox.Labels = make(map[string]string)
	}
	setOrDeleteLabel(sandbox.Labels, sandboxTemplateRefHash, templateHash)
	setOrDeleteLabel(sandbox.Labels, v1beta1.CreatedByLabel, createdBy)

	logger.V(1).Info("Updating sandbox metadata to match claim", "claim", claim.Name, "sandbox", sandbox.Name)
	sandbox.Spec.PodTemplate.ObjectMeta = mergedMeta
	if err := r.Patch(ctx, sandbox, patch); err != nil {
		return fmt.Errorf("failed to patch sandbox metadata for claim %q: %w", claim.Name, err)
	}
	return nil
}

// checkExpiration calculates if the claim is expired and how much time is left.
func (r *SandboxClaimReconciler) checkExpiration(claim *extensionsv1beta1.SandboxClaim) (bool, time.Duration) {
	if claim.Spec.Lifecycle == nil {
		return false, 0
	}

	finishedCondition := lifecycle.FinishedCondition(claim.Status.Conditions, string(v1beta1.SandboxConditionFinished))
	return lifecycle.TimeLeft(time.Now(), claim.Spec.Lifecycle.ShutdownTime, claim.Spec.Lifecycle.TTLSecondsAfterFinished, finishedCondition)
}

// reconcileActive handles the creation and updates of running sandboxes.
func (r *SandboxClaimReconciler) reconcileActive(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconciling active claim", "claim", claim.Name)

	if err := r.validateAdditionalPodMetadata(&claim.Spec.AdditionalPodMetadata); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	sandbox, err := r.getOrCreateSandbox(ctx, claim)
	logger.V(1).Info("getOrCreateSandbox result", "sandboxFound", sandbox != nil, "err", err, "claim", claim.Name)
	if err != nil {
		return nil, err
	}
	if sandbox != nil {
		return sandbox, r.syncAdoptedSandboxMetadata(ctx, claim, sandbox)
	}

	// Need template to create from scratch.
	logger.V(1).Info("Cold path: no sandbox found, creating from template", "claim", claim.Name)
	template, templateErr := r.getTemplate(ctx, claim)
	if templateErr != nil {
		return nil, templateErr
	}

	return r.createSandbox(ctx, claim, template)
}

// reconcileExpired ensures the Sandbox is deleted for Retained claims.
func (r *SandboxClaimReconciler) reconcileExpired(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconciling Expired claim", "claim", claim.Name)

	// Fall back to claim.Name when status is unset.
	statusName := claim.Name
	if claim.Status.SandboxStatus.Name != "" {
		statusName = claim.Status.SandboxStatus.Name
	}

	sandbox := &v1beta1.Sandbox{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: statusName}, sandbox); err != nil {
		if k8errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if !metav1.IsControlledBy(sandbox, claim) {
		logger.Info("Skipping deletion: Sandbox is not controlled by this claim", "sandbox", sandbox.Name, "claim", claim.Name)
		return nil, fmt.Errorf("%w: sandbox %q is not owned by claim %q", ErrSandboxNotOwned, sandbox.Name, claim.Name)
	}
	if sandbox.DeletionTimestamp.IsZero() {
		logger.Info("Deleting Sandbox because Claim expired (Policy=Retain)", "sandbox", sandbox.Name, "claim", claim.Name)
		if err := r.Delete(ctx, sandbox); err != nil {
			return sandbox, fmt.Errorf("failed to delete expired sandbox: %w", err)
		}
	}
	return sandbox, nil
}

func (r *SandboxClaimReconciler) updateStatus(ctx context.Context, oldStatus *extensionsv1beta1.SandboxClaimStatus, claim *extensionsv1beta1.SandboxClaim) error {
	logger := log.FromContext(ctx)

	slices.SortFunc(oldStatus.Conditions, func(a, b metav1.Condition) int { return cmp.Compare(a.Type, b.Type) })
	slices.SortFunc(claim.Status.Conditions, func(a, b metav1.Condition) int { return cmp.Compare(a.Type, b.Type) })

	if equality.Semantic.DeepEqual(oldStatus, &claim.Status) {
		return nil
	}

	oldClaim := claim.DeepCopy()
	oldClaim.Status = *oldStatus

	patch := client.MergeFrom(oldClaim)

	if err := r.Status().Patch(ctx, claim, patch); err != nil {
		logger.Error(err, "Failed to patch sandboxclaim status")
		return err
	}

	logger.V(4).Info("Successfully patched sandboxclaim status",
		"name", claim.Name,
		"namespace", claim.Namespace,
		"observedGeneration", claim.Generation)
	return nil
}

func (r *SandboxClaimReconciler) computeReadyCondition(claim *extensionsv1beta1.SandboxClaim, sandbox *v1beta1.Sandbox, err error, isClaimExpired bool) metav1.Condition {
	if err != nil {
		return notReady(claim, readyFailure(claim, err))
	}

	if isClaimExpired {
		return notReady(claim, failure{reason: extensionsv1beta1.ClaimExpiredReason, message: "Claim expired. Sandbox cleanup initiated."})
	}

	if sandbox == nil {
		// Only handle genuine missing sandbox here (expired case is handled above)
		return notReady(claim, failure{reason: "SandboxMissing", message: "Sandbox does not exist"})
	}

	if readyReasonIs(sandbox.Status.Conditions, v1beta1.SandboxReasonExpired) {
		return notReady(claim, failure{reason: v1beta1.SandboxReasonExpired, message: "Underlying Sandbox resource has expired independently of the Claim."})
	}

	// Forward the condition from Sandbox Status
	for _, condition := range sandbox.Status.Conditions {
		if condition.Type == string(v1beta1.SandboxConditionReady) {
			return condition
		}
	}

	return metav1.Condition{
		Type:               string(v1beta1.SandboxConditionReady),
		Status:             metav1.ConditionFalse,
		Reason:             reasonSandboxNotReady,
		Message:            msgSandboxNotReady,
		ObservedGeneration: claim.Generation,
	}
}

func (r *SandboxClaimReconciler) computeAndSetStatus(claim *extensionsv1beta1.SandboxClaim, sandbox *v1beta1.Sandbox, err error, isClaimExpired bool) {
	// A cache-lag adoption retry is a benign look-again, not a state change. If the
	// claim status was already finalized with a sandbox (the adoption pass itself, or
	// a controller restart racing a stale informer), leave the recorded Name/PodIPs
	// and existing conditions untouched instead of transiently wiping them.
	if sandbox == nil && errors.Is(err, errAdoptionTriggeredRetry) && claim.Status.SandboxStatus.Name != "" {
		return
	}
	readyCondition := r.computeReadyCondition(claim, sandbox, err, isClaimExpired)
	meta.SetStatusCondition(&claim.Status.Conditions, readyCondition)
	r.syncFinishedCondition(claim, sandbox, isClaimExpired)

	if sandbox != nil {
		claim.Status.SandboxStatus.Name = sandbox.Name
		claim.Status.SandboxStatus.PodIPs = sandbox.Status.PodIPs
	} else if err == nil || errors.Is(err, ErrSandboxNotOwned) {
		// Only clear bound sandbox identity when there is no error (sandbox legitimately deleted or unbound)
		// or when ownership verification fails. Never clear on transient lookup or patch errors, as wiping
		// status.sandbox.name forces a fallback to cold-start on the next reconcile retry.
		claim.Status.SandboxStatus.Name = ""
		claim.Status.SandboxStatus.PodIPs = nil
	}
}

func (r *SandboxClaimReconciler) syncFinishedCondition(claim *extensionsv1beta1.SandboxClaim, sandbox *v1beta1.Sandbox, isClaimExpired bool) {
	if sandbox != nil {
		finishedCondition := meta.FindStatusCondition(sandbox.Status.Conditions, string(v1beta1.SandboxConditionFinished))
		if finishedCondition != nil {
			meta.SetStatusCondition(&claim.Status.Conditions, *finishedCondition)
		} else {
			meta.RemoveStatusCondition(&claim.Status.Conditions, string(v1beta1.SandboxConditionFinished))
		}
		return
	}

	if !isClaimExpired {
		meta.RemoveStatusCondition(&claim.Status.Conditions, string(v1beta1.SandboxConditionFinished))
	}
}

func (r *SandboxClaimReconciler) getCandidate(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, queue.SandboxKey, error) {
	logger := log.FromContext(ctx)

	namespacedWarmPoolName := queue.GetNamespacedWarmPoolName(claim.Namespace, claim.Spec.WarmPoolRef.Name)

	var skipped []queue.SandboxKey
	var fallbackSandbox *v1beta1.Sandbox
	var fallbackKey queue.SandboxKey
	var adoptingFallback bool

	// Skipped and unadopted-fallback keys must go back in the queue or their
	// warm sandboxes would be stranded until the next pool resync.
	defer func() {
		for _, key := range skipped {
			r.WarmSandboxQueue.Add(namespacedWarmPoolName, key)
		}
		if fallbackSandbox != nil && !adoptingFallback {
			r.WarmSandboxQueue.Add(namespacedWarmPoolName, fallbackKey)
		}
	}()

	for {
		adoptedKey, ok := r.WarmSandboxQueue.GetWithStrategy(namespacedWarmPoolName, pickNodeSpread)
		if !ok {
			if fallbackSandbox != nil {
				adoptingFallback = true
				return fallbackSandbox, fallbackKey, nil
			}
			return nil, queue.SandboxKey{}, nil
		}

		adopted := &v1beta1.Sandbox{}
		err := r.Get(ctx, client.ObjectKey{Namespace: adoptedKey.Namespace, Name: adoptedKey.Name}, adopted)
		if err != nil {
			if k8errors.IsNotFound(err) {
				// The queue can hold keys for sandboxes already deleted from the cluster.
				continue
			}
			r.WarmSandboxQueue.Add(namespacedWarmPoolName, adoptedKey)
			return nil, queue.SandboxKey{}, err
		}

		if err := verifySandboxCandidate(adopted, claim); err != nil {
			logger.V(1).Info("sandbox candidate can't be adopted", "sandbox", adopted.Name, "warmPool", claim.Spec.WarmPoolRef.Name, "reason", err.Error())
			continue
		}

		if isSandboxReady(adopted) {
			return adopted, adoptedKey, nil
		}

		// The first unready-but-valid sandbox is kept as the fallback adoption.
		if fallbackSandbox == nil {
			fallbackSandbox = adopted
			fallbackKey = adoptedKey
		} else {
			skipped = append(skipped, adoptedKey)
		}
	}
}

func (r *SandboxClaimReconciler) adoptSandboxFromCandidates(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	namespacedWarmPoolNameForQueue := queue.GetNamespacedWarmPoolName(claim.Namespace, claim.Spec.WarmPoolRef.Name)

	for range 3 {
		adopted, adoptedKey, err := r.getCandidate(ctx, claim)
		if err != nil {
			return nil, err
		}
		if adopted == nil {
			logger.Info("Failed to adopt any sandbox after checking all candidates", "claim", claim.Name)
			return nil, nil // Warm pool is truly empty, fall completely to cold start
		}

		success, err := r.tryAdopt(ctx, claim, adopted, adoptedKey, namespacedWarmPoolNameForQueue)
		if err != nil {
			return nil, err
		}

		if success {
			return adopted, nil
		}
	}

	logger.Info("Failed to adopt sandbox after max retries", "claim", claim.Name)
	return nil, nil
}

// tryAdopt records the assignment on the claim and hands the Sandbox over. It
// reports false when the candidate was lost to a competing claim or vanished,
// which the caller answers by trying the next candidate. The claim Update is a
// full-object CAS on purpose: it is what keeps one claim from adopting twice.
func (r *SandboxClaimReconciler) tryAdopt(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, adopted *v1beta1.Sandbox, adoptedKey queue.SandboxKey, queueName string) (bool, error) {
	logger := log.FromContext(ctx)
	poolName := "none"
	if wpName := getWarmPoolName(adopted); wpName != "" {
		poolName = wpName
	}
	logger.Info("Attempting sandbox adoption", "sandbox candidate", adopted.Name, "warm pool", poolName, "claim", claim.Name)

	if claim.Annotations == nil {
		claim.Annotations = make(map[string]string)
	}
	claim.Annotations[extensionsv1beta1.AssignedSandboxNameAnnotation] = adopted.Name
	if updateErr := r.Update(ctx, claim); updateErr != nil {
		r.WarmSandboxQueue.Add(queueName, adoptedKey)
		if !k8errors.IsConflict(updateErr) {
			logger.Error(updateErr, "Failed to update claim for adoption", "claim", claim.Name, "sandbox", adopted.Name)
		}
		return false, updateErr
	}

	if adoptErr := r.completeAdoption(ctx, claim, adopted); adoptErr != nil {
		if k8errors.IsNotFound(adoptErr) {
			return false, nil
		}
		r.WarmSandboxQueue.Add(queueName, adoptedKey)
		if k8errors.IsConflict(adoptErr) {
			return false, nil
		}
		logger.Error(adoptErr, "Failed to complete adoption for candidate sandbox", "sandbox candidate", adopted.Name, "claim", claim.Name)
		return false, adoptErr
	}

	logger.Info("Successfully adopted sandbox from warm pool", "sandbox", adopted.Name, "claim", claim.Name)
	r.triggeredAdoptions.Store(
		types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace},
		triggeredAdoptionEntry{uid: claim.UID, sandbox: adopted.Name},
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, "SandboxAdopted", "Adoption", "Adopted warm pool Sandbox %q", adopted.Name)
	}

	podCondition := "not_ready"
	if isSandboxReady(adopted) {
		podCondition = "ready"
	}
	asmetrics.RecordSandboxClaimCreation(claim.Namespace, r.resolveTemplateName(adopted), asmetrics.LaunchTypeWarm, poolName, podCondition, claim.Labels[v1beta1.CreatedByLabel])
	return true, nil
}

func (r *SandboxClaimReconciler) completeAdoption(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, adopted *v1beta1.Sandbox) error {
	originalAdopted := adopted.DeepCopy()

	templateHash := adopted.Labels[sandboxTemplateRefHash]

	if adopted.Labels == nil {
		adopted.Labels = make(map[string]string)
	}
	delete(adopted.Labels, warmPoolSandboxLabel)
	delete(adopted.Labels, v1beta1.SandboxTemplateHashLabel)
	adopted.Labels[v1beta1.SandboxLaunchTypeLabel] = v1beta1.SandboxLaunchTypeWarm
	// Remove the warm pool's default eviction annotation so the adopted sandbox
	// is protected from autoscaler scale-downs now that it hosts active state.
	// Custom template-specified overrides (e.g. "false") are explicitly kept.
	if adopted.Spec.PodTemplate.ObjectMeta.Annotations != nil && adopted.Spec.PodTemplate.ObjectMeta.Annotations[warmPoolEvictionAnnotation] == "true" {
		delete(adopted.Spec.PodTemplate.ObjectMeta.Annotations, warmPoolEvictionAnnotation)
	}

	adopted.OwnerReferences = nil
	if err := controllerutil.SetControllerReference(claim, adopted, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on adopted sandbox: %w", err)
	}

	if adopted.Annotations == nil {
		adopted.Annotations = make(map[string]string)
	}

	// Ensure the adopted sandbox records its pod name before it can be observed Ready.
	if podName := adopted.Annotations[v1beta1.SandboxPodNameAnnotation]; podName != adopted.Name {
		adopted.Annotations[v1beta1.SandboxPodNameAnnotation] = adopted.Name
	}

	if traceContext, ok := claim.Annotations[asmetrics.TraceContextAnnotation]; ok {
		adopted.Annotations[asmetrics.TraceContextAnnotation] = traceContext
	}

	adopted.Labels = ensureClaimIdentityLabels(adopted.Labels, claim)
	adopted.Spec.PodTemplate.ObjectMeta.Labels = ensureClaimIdentityLabels(adopted.Spec.PodTemplate.ObjectMeta.Labels, claim)

	template, templateErr := r.getTemplate(ctx, claim)
	if templateHash == "" && template != nil {
		templateHash = SandboxTemplateRefHash(template.Name)
	} else if templateHash == "" && templateErr != nil {
		log.FromContext(ctx).V(1).Info("Unable to set template ref hash label during adoption because template lookup failed", "sandbox", adopted.Name, "claim", claim.Name, "error", templateErr.Error())
	}

	// Keep the template ref hash on the adopted sandbox's top-level labels so
	// discovery by template hash keeps working after adoption.
	if templateHash != "" {
		adopted.Labels[sandboxTemplateRefHash] = templateHash
	}

	// Without a template there is nothing to reset to, so the pre-warmed pod
	// metadata is amended in place instead of replaced.
	target := &adopted.Spec.PodTemplate.ObjectMeta
	if templateErr == nil && template != nil {
		var mergedMeta v1beta1.PodMetadata
		template.Spec.PodTemplate.ObjectMeta.DeepCopyInto(&mergedMeta)
		if mergedMeta.Labels == nil {
			mergedMeta.Labels = make(map[string]string)
		}
		mergedMeta.Labels[extensionsv1beta1.SandboxIDLabel] = string(claim.UID)
		setOrDeleteLabel(mergedMeta.Labels, v1beta1.CreatedByLabel, claim.Labels[v1beta1.CreatedByLabel])
		adopted.Spec.PodTemplate.ObjectMeta = mergedMeta
	}
	if templateHash != "" {
		target.Labels[sandboxTemplateRefHash] = templateHash
	}
	if err := r.mergePodMetadata(target, &claim.Spec.AdditionalPodMetadata); err != nil {
		return err
	}

	if err := r.Patch(ctx, adopted, client.MergeFrom(originalAdopted)); err != nil {
		return err
	}

	return nil
}

// validateAdditionalPodMetadata checks claimMeta for invalid domain or label values upfront.
func (r *SandboxClaimReconciler) validateAdditionalPodMetadata(claimMeta *v1beta1.PodMetadata) error {
	if claimMeta == nil {
		return nil
	}

	allowedDomains := r.AllowedLabelDomains
	if len(allowedDomains) == 0 {
		allowedDomains = []string{"sandbox.users.io"} // Secure default fallback
	}

	validate := func(key, value string, isLabel bool) error {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			kind := "annotation"
			if isLabel {
				kind = "label"
			}
			return fmt.Errorf("invalid %s key: %q: %s", kind, key, strings.Join(errs, "; "))
		}

		// Block spoofing of system components
		if isLabel && strings.EqualFold(key, "app") && strings.EqualFold(value, "sandbox-router") {
			return fmt.Errorf("restricted system label value: %q=%q is not allowed in AdditionalPodMetadata", key, value)
		}

		parts := strings.SplitN(key, "/", 2)
		domain := ""
		if len(parts) > 1 {
			domain = strings.ToLower(parts[0])
		} else if isLabel {
			return fmt.Errorf("label %q must have a domain prefix (e.g. 'sandbox.users.io/my-label') to prevent opting into unintended policy domains", key)
		}

		if isLabel {
			// Strict Allowlist for labels
			if !domainInList(domain, allowedDomains) {
				return fmt.Errorf("label domain %q is not in the allowlist", domain)
			}
		} else if isRestrictedDomain(domain) {
			return fmt.Errorf("restricted system domain: %q is not allowed in AdditionalPodMetadata", key)
		}

		// Validate label values (annotations have less restrictions)
		if isLabel {
			if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
				return fmt.Errorf("invalid label value: %q does not match allowed pattern: %s", value, strings.Join(errs, "; "))
			}
		}
		return nil
	}

	for k, v := range claimMeta.Labels {
		if err := validate(k, v, true); err != nil {
			return fmt.Errorf("failed to validate label %q: %w", k, err)
		}
	}

	for k, v := range claimMeta.Annotations {
		if err := validate(k, v, false); err != nil {
			return fmt.Errorf("failed to validate annotation %q: %w", k, err)
		}
	}

	return nil
}

// mergePodMetadata merges labels and annotations from claimMeta into templateMeta,
// rejecting overrides with different values.
func (r *SandboxClaimReconciler) mergePodMetadata(templateMeta *v1beta1.PodMetadata, claimMeta *v1beta1.PodMetadata) error {
	if err := r.validateAdditionalPodMetadata(claimMeta); err != nil {
		return err
	}

	for k, v := range claimMeta.Labels {
		if tv, ok := templateMeta.Labels[k]; ok && tv != v {
			return fmt.Errorf("metadata override conflict: label %q is defined in template with value %q, but claim requests %q", k, tv, v)
		}
	}

	for k, v := range claimMeta.Annotations {
		if tv, ok := templateMeta.Annotations[k]; ok && tv != v {
			return fmt.Errorf("metadata override conflict: annotation %q is defined in template with value %q, but claim requests %q", k, tv, v)
		}
	}

	if len(claimMeta.Labels) > 0 {
		if templateMeta.Labels == nil {
			templateMeta.Labels = make(map[string]string)
		}
		maps.Copy(templateMeta.Labels, claimMeta.Labels)
	}

	if len(claimMeta.Annotations) > 0 {
		if templateMeta.Annotations == nil {
			templateMeta.Annotations = make(map[string]string)
		}
		maps.Copy(templateMeta.Annotations, claimMeta.Annotations)
	}

	return nil
}

func (r *SandboxClaimReconciler) injectEnvs(logger logr.Logger, container *corev1.Container, envsToInject []extensionsv1beta1.EnvVar, policy extensionsv1beta1.EnvVarsInjectionPolicy, claimName string) error {
	if policy == extensionsv1beta1.EnvVarsInjectionPolicyAllowed && len(container.EnvFrom) > 0 {
		return fmt.Errorf("%w: container %q uses EnvFrom sources; Allowed policy cannot safely prevent overriding EnvFrom-provided variables", ErrEnvVarsInjectionRejected, container.Name)
	}

	for _, claimEnv := range envsToInject {
		existingIdx := -1
		for j, env := range container.Env {
			if env.Name == claimEnv.Name {
				existingIdx = j
				break
			}
		}

		if existingIdx >= 0 {
			if policy != extensionsv1beta1.EnvVarsInjectionPolicyOverrides {
				err := fmt.Errorf("environment variable override is not allowed by the template policy for variable %q", claimEnv.Name)
				logger.Error(err, "Environment variable override rejected", "claimName", claimName, "envName", claimEnv.Name)
				return err
			}
			logger.Info("Overriding existing environment variable", "envName", claimEnv.Name, "container", container.Name)
			container.Env[existingIdx] = corev1.EnvVar{Name: claimEnv.Name, Value: claimEnv.Value}
		} else {
			logger.Info("Appending new environment variable", "envName", claimEnv.Name, "container", container.Name)
			container.Env = append(container.Env, corev1.EnvVar{Name: claimEnv.Name, Value: claimEnv.Value})
		}
	}
	return nil
}

func (r *SandboxClaimReconciler) createSandbox(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, template *extensionsv1beta1.SandboxTemplate) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)

	if template == nil {
		logger.Error(ErrTemplateNotFound, "cannot create sandbox: template of the warmpool not found", "warmPool", claim.Spec.WarmPoolRef.Name)
		return nil, ErrTemplateNotFound
	}

	logger.Info("creating sandbox from template", "template", template.Name)
	sandbox := &v1beta1.Sandbox{
		Namespace: claim.Namespace,
		Name:      claim.Name,
	}

	if sandbox.Annotations == nil {
		sandbox.Annotations = make(map[string]string)
	}
	if traceContext, ok := claim.Annotations[asmetrics.TraceContextAnnotation]; ok {
		sandbox.Annotations[asmetrics.TraceContextAnnotation] = traceContext
	}

	// Track the sandbox template ref to be used by metrics collector
	sandbox.Annotations[v1beta1.SandboxTemplateRefAnnotation] = template.Name

	sandbox.Spec.SandboxBlueprint = *template.Spec.SandboxBlueprint.DeepCopy()
	// Merge volumeClaimTemplates from template and claim according to the template policy
	if len(claim.Spec.VolumeClaimTemplates) > 0 {
		resolvedVCTs, err := mergeVolumeClaimTemplates(
			template.Spec.VolumeClaimTemplates,
			claim.Spec.VolumeClaimTemplates,
			template.Spec.VolumeClaimTemplatesPolicy,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to merge volume claim templates: %w", err)
		}
		if len(resolvedVCTs) > 0 {
			sandbox.Spec.VolumeClaimTemplates = make([]v1beta1.PersistentVolumeClaimTemplate, len(resolvedVCTs))
			for i, vct := range resolvedVCTs {
				vct.DeepCopyInto(&sandbox.Spec.VolumeClaimTemplates[i])
			}
		}
	} else {
		if err := validateVolumeClaimTemplates(template.Spec.VolumeClaimTemplates); err != nil {
			return nil, fmt.Errorf("invalid volume claim templates in template: %w", err)
		}
	}

	templateHash := SandboxTemplateRefHash(template.Name)
	sandbox.Labels = ensureClaimIdentityLabels(sandbox.Labels, claim)
	sandbox.Labels[v1beta1.SandboxLaunchTypeLabel] = v1beta1.SandboxLaunchTypeCold
	sandbox.Labels[sandboxTemplateRefHash] = templateHash
	sandbox.Spec.PodTemplate.ObjectMeta.Labels = ensureClaimIdentityLabels(sandbox.Spec.PodTemplate.ObjectMeta.Labels, claim)
	sandbox.Spec.PodTemplate.ObjectMeta.Labels[sandboxTemplateRefHash] = templateHash

	if err := r.mergePodMetadata(&sandbox.Spec.PodTemplate.ObjectMeta, &claim.Spec.AdditionalPodMetadata); err != nil {
		return nil, err
	}

	if err := r.injectClaimEnv(logger, claim, template, &sandbox.Spec.PodTemplate.Spec); err != nil {
		return nil, err
	}

	ApplySandboxSecureDefaults(template, &sandbox.Spec.PodTemplate.Spec)

	if err := controllerutil.SetControllerReference(claim, sandbox, r.Scheme); err != nil {
		err = fmt.Errorf("failed to set controller reference for sandbox: %w", err)
		logger.Error(err, "Error creating sandbox for claim", "claimName", claim.Name)
		return nil, err
	}

	if err := r.Create(ctx, sandbox); err != nil {
		err = fmt.Errorf("sandbox create error: %w", err)
		logger.Error(err, "Error creating sandbox for claim", "claimName", claim.Name)
		return nil, err
	}

	logger.Info("Created sandbox for claim", "claim", claim.Name, "sandbox", sandbox.Name, "isReady", false, "duration", time.Since(claim.CreationTimestamp.Time))

	if r.Recorder != nil {
		r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, "SandboxProvisioned", "Provisioning", "Created Sandbox %q", sandbox.Name)
	}

	asmetrics.RecordSandboxClaimCreation(claim.Namespace, template.Name, asmetrics.LaunchTypeCold, claim.Spec.WarmPoolRef.Name, "not_ready", claim.Labels[v1beta1.CreatedByLabel])

	return sandbox, nil
}

// injectClaimEnv applies the claim's environment variables to the pod spec. An
// unnamed container targets the first main container; a named one that the
// template does not declare is rejected rather than silently dropped.
func (r *SandboxClaimReconciler) injectClaimEnv(logger logr.Logger, claim *extensionsv1beta1.SandboxClaim, template *extensionsv1beta1.SandboxTemplate, spec *corev1.PodSpec) error {
	if len(claim.Spec.Env) == 0 {
		return nil
	}
	policy := template.Spec.EnvVarsInjectionPolicy
	if policy != extensionsv1beta1.EnvVarsInjectionPolicyAllowed && policy != extensionsv1beta1.EnvVarsInjectionPolicyOverrides {
		err := fmt.Errorf("%w: environment variable injection is not allowed by the template policy", ErrEnvVarsInjectionRejected)
		logger.Error(err, "Environment variable injection rejected", "claimName", claim.Name)
		return err
	}

	envsByContainer := make(map[string][]extensionsv1beta1.EnvVar)
	var defaultEnvs []extensionsv1beta1.EnvVar
	for _, env := range claim.Spec.Env {
		if env.ContainerName == "" {
			defaultEnvs = append(defaultEnvs, env)
			continue
		}
		envsByContainer[env.ContainerName] = append(envsByContainer[env.ContainerName], env)
	}

	declared := make(map[string]struct{}, len(spec.InitContainers)+len(spec.Containers))
	for _, c := range slices.Concat(spec.InitContainers, spec.Containers) {
		declared[c.Name] = struct{}{}
	}
	for name, envs := range envsByContainer {
		if _, ok := declared[name]; ok {
			continue
		}
		err := fmt.Errorf("target container %q not found in template", name)
		if len(envs) > 0 {
			err = fmt.Errorf("target container %q not found in template for environment variable %q", name, envs[0].Name)
		}
		logger.Error(err, "Environment variable injection rejected: container not found", "claimName", claim.Name)
		return err
	}

	for i := range spec.InitContainers {
		container := &spec.InitContainers[i]
		if envs := envsByContainer[container.Name]; len(envs) > 0 {
			if err := r.injectEnvs(logger, container, envs, policy, claim.Name); err != nil {
				return err
			}
		}
	}
	for i := range spec.Containers {
		container := &spec.Containers[i]
		envs := envsByContainer[container.Name]
		if i == 0 {
			envs = slices.Concat(envs, defaultEnvs)
		}
		if len(envs) > 0 {
			if err := r.injectEnvs(logger, container, envs, policy, claim.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *SandboxClaimReconciler) getOrCreateSandbox(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Executing getOrCreateSandbox", "claim", claim.Name)

	for _, resolve := range []func(context.Context, *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error){
		r.sandboxFromStatus,
		r.sandboxFromClaimMetadata,
		r.sandboxByClaimName,
	} {
		sandbox, err := resolve(ctx, claim)
		if err != nil || sandbox != nil {
			return sandbox, err
		}
	}

	// Custom configuration cannot be served from the warm pool, so skip the queue
	// entirely and let the caller cold-start.
	if len(claim.Spec.Env) > 0 || len(claim.Spec.VolumeClaimTemplates) > 0 {
		logger.Info("Bypassing warm pool adoption because custom configuration is provided (env or volume claim templates)", "claim", claim.Name)
		return nil, nil
	}
	return r.adoptSandboxFromCandidates(ctx, claim)
}

// sandboxFromStatus returns the Sandbox recorded in claim status, when this claim
// still controls it. A nil Sandbox and a nil error mean "not resolved here".
func (r *SandboxClaimReconciler) sandboxFromStatus(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	statusName := claim.Status.SandboxStatus.Name
	if statusName == "" {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.V(1).Info("Checking status for sandbox name", "claim.Status.SandboxStatus.Name", statusName, "claim", claim.Name)

	sandbox := &v1beta1.Sandbox{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: statusName}, sandbox); err != nil {
		if k8errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get sandbox %q from status: %w", statusName, err)
	}
	if !metav1.IsControlledBy(sandbox, claim) {
		return nil, nil
	}

	logger.V(4).Info("Found existing adopted sandbox from status", "claim.Status.SandboxStatus.Name", statusName, "claim", claim.Name)
	r.triggeredAdoptions.Delete(types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace})
	launchType := v1beta1.SandboxLaunchTypeCold
	if claim.Annotations[extensionsv1beta1.AssignedSandboxNameAnnotation] == statusName ||
		statusName != claim.Name {
		launchType = v1beta1.SandboxLaunchTypeWarm
	}
	if err := r.initializeSandboxLaunchTypeLabel(ctx, sandbox, launchType); err != nil {
		return nil, fmt.Errorf("failed to initialize launch type label on sandbox %q: %w", sandbox.Name, err)
	}
	return sandbox, nil
}

// sandboxFromClaimMetadata resolves the assigned-sandbox annotation. A Sandbox
// still owned by the warm pool means an adoption is half-finished, which this
// completes.
func (r *SandboxClaimReconciler) sandboxFromClaimMetadata(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	sbName := claim.Annotations[extensionsv1beta1.AssignedSandboxNameAnnotation]
	if sbName == "" {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.V(1).Info("Checking assigned sandbox name", "sandboxName", sbName, "claim", claim.Name)

	sandbox := &v1beta1.Sandbox{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: sbName}, sandbox); err != nil {
		if !k8errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get sandbox %q for sandbox name lookup: %w", sbName, err)
		}
		logger.Info("Sandbox recorded in claim metadata not found, removing stale reference", "sandboxName", sbName, "claim", claim.Name)
		if err := r.clearAssignedSandboxName(ctx, claim); err != nil {
			return nil, fmt.Errorf("failed to remove stale sandbox reference from claim metadata: %w", err)
		}
		logger.Info("Successfully removed stale sandbox reference from claim metadata", "sandbox", sbName, "claim", claim.Name)
		return nil, nil
	}

	if metav1.IsControlledBy(sandbox, claim) {
		logger.V(4).Info("Found existing adopted sandbox", "sandbox", sbName, "claim", claim.Name)
		r.triggeredAdoptions.Delete(types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace})
		if err := r.initializeSandboxLaunchTypeLabel(ctx, sandbox, v1beta1.SandboxLaunchTypeWarm); err != nil {
			return nil, fmt.Errorf("failed to initialize launch type label on sandbox %q: %w", sandbox.Name, err)
		}
		return sandbox, nil
	}

	if ref := metav1.GetControllerOf(sandbox); ref != nil && ref.Kind == warmPoolKind {
		return nil, r.completePendingAdoption(ctx, claim, sandbox, sbName)
	}
	logger.V(4).Info("Sandbox recorded in claim metadata belongs to another claim, falling through", "sandbox", sbName, "claim", claim.Name)
	return nil, nil
}

// completePendingAdoption finishes an adoption the claim already recorded. It never
// returns a Sandbox: the caller must requeue so a later pass observes the adopted
// Sandbox once the informer cache converges.
func (r *SandboxClaimReconciler) completePendingAdoption(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, sandbox *v1beta1.Sandbox, sbName string) error {
	logger := log.FromContext(ctx)
	logger.Info("Sandbox found in claim metadata still in warm pool, trying to complete adoption", "sandbox", sbName, "claim", claim.Name)

	if err := verifySandboxCandidate(sandbox, claim); err != nil {
		logger.Info("Sandbox recorded in claim metadata cannot be adopted, removing stale reference", "sandboxName", sbName, "claim", claim.Name, "reason", err.Error())
		if err := r.clearAssignedSandboxName(ctx, claim); err != nil {
			return fmt.Errorf("failed to remove invalid sandbox reference: %w", err)
		}
		return nil
	}

	adoptionKey := types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}
	if prev, ok := r.triggeredAdoptions.Load(adoptionKey); ok && prev.uid == claim.UID && prev.sandbox == sbName {
		logger.V(4).Info("Adoption already triggered, waiting for cache to converge", "sandbox", sbName, "claim", claim.Name)
		return fmt.Errorf("%w: sandbox %s", errAdoptionTriggeredRetry, sbName)
	}

	if err := r.completeAdoption(ctx, claim, sandbox); err != nil {
		if !k8errors.IsNotFound(err) && !k8errors.IsConflict(err) {
			return fmt.Errorf("failed to complete adoption of %q: %w", sbName, err)
		}
		logger.V(4).Info("Failed to complete adoption (conflict/notfound), falling through", "sandbox", sbName, "claim", claim.Name)
		return nil
	}

	r.triggeredAdoptions.Store(adoptionKey, triggeredAdoptionEntry{uid: claim.UID, sandbox: sbName})
	logger.Info("Triggered adoption completion for sandbox, requeueing", "sandbox", sbName, "claim", claim.Name)
	return fmt.Errorf("%w: sandbox %s", errAdoptionTriggeredRetry, sbName)
}

// sandboxByClaimName finds the Sandbox that createSandbox would have made, which
// carries the claim's own name.
func (r *SandboxClaimReconciler) sandboxByClaimName(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*v1beta1.Sandbox, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Trying name-based lookup for sandbox", "claim", claim.Name)

	sandbox := &v1beta1.Sandbox{
		Namespace: claim.Namespace, Name: claim.Name,
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sandbox), sandbox); err != nil {
		if k8errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get sandbox %q: %w", claim.Name, err)
	}

	logger.V(4).Info("sandbox already exists, skipping update", "name", sandbox.Name)
	if !metav1.IsControlledBy(sandbox, claim) {
		err := fmt.Errorf("sandbox %q is not controlled by claim %q. Please use a different claim name or delete the sandbox manually", sandbox.Name, claim.Name)
		logger.Error(err, "Sandbox controller mismatch")
		return nil, err
	}
	if err := r.initializeSandboxLaunchTypeLabel(ctx, sandbox, v1beta1.SandboxLaunchTypeCold); err != nil {
		return nil, fmt.Errorf("failed to initialize launch type label on sandbox %q: %w", sandbox.Name, err)
	}
	return sandbox, nil
}

// clearAssignedSandboxName drops a claim's reference to a Sandbox that no longer
// exists or can no longer be adopted.
func (r *SandboxClaimReconciler) clearAssignedSandboxName(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) error {
	patch := client.MergeFrom(claim.DeepCopy())
	delete(claim.Annotations, extensionsv1beta1.AssignedSandboxNameAnnotation)
	return r.Patch(ctx, claim, patch)
}

func (r *SandboxClaimReconciler) initializeSandboxLaunchTypeLabel(ctx context.Context, sandbox *v1beta1.Sandbox, launchType string) error {
	if sandbox.Labels != nil {
		if _, ok := sandbox.Labels[v1beta1.SandboxLaunchTypeLabel]; ok {
			return nil
		}
	}

	patch := client.MergeFrom(sandbox.DeepCopy())
	if sandbox.Labels == nil {
		sandbox.Labels = make(map[string]string)
	}
	sandbox.Labels[v1beta1.SandboxLaunchTypeLabel] = launchType
	return r.Patch(ctx, sandbox, patch)
}

func (r *SandboxClaimReconciler) getTemplate(ctx context.Context, claim *extensionsv1beta1.SandboxClaim) (*extensionsv1beta1.SandboxTemplate, error) {
	warmPool := &extensionsv1beta1.SandboxWarmPool{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: claim.Spec.WarmPoolRef.Name}, warmPool); err != nil {
		if k8errors.IsNotFound(err) {
			return nil, ErrWarmPoolNotFound
		}
		return nil, fmt.Errorf("failed to get sandbox warm pool %q: %w", claim.Spec.WarmPoolRef.Name, err)
	}

	template := &extensionsv1beta1.SandboxTemplate{
		Namespace: claim.Namespace,
		Name:      warmPool.Spec.TemplateRef.Name,
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(template), template); err != nil {
		if k8errors.IsNotFound(err) {
			return nil, fmt.Errorf(`SandboxTemplate %q not found: %w`, warmPool.Spec.TemplateRef.Name, ErrTemplateNotFound)
		}
		return nil, fmt.Errorf("failed to get sandbox template %q: %w", warmPool.Spec.TemplateRef.Name, err)
	}

	return template, nil
}

// resolveTemplateName safely extracts the SandboxTemplate name from the Sandbox annotations.
func (r *SandboxClaimReconciler) resolveTemplateName(sandbox *v1beta1.Sandbox) string {
	if sandbox != nil && sandbox.Annotations != nil && sandbox.Annotations[v1beta1.SandboxTemplateRefAnnotation] != "" {
		return sandbox.Annotations[v1beta1.SandboxTemplateRefAnnotation]
	}
	return "__unknown__"
}

// getOrRecordObservedTime stores the first time an object is seen by the controller in an in-memory
// map observedTimes for latency tracking. It returns the resolved timestamp for the object.
func (r *SandboxClaimReconciler) getOrRecordObservedTime(obj client.Object) time.Time {
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}

	if entry, ok := r.observedTimes.Load(key); ok {
		if entry.uid == obj.GetUID() {
			return entry.timestamp
		}
	}

	newEntry := observedTimeEntry{timestamp: time.Now(), uid: obj.GetUID()}
	actual, loaded := r.observedTimes.LoadOrStore(key, newEntry)
	if loaded {
		if actual.uid != obj.GetUID() {
			r.observedTimes.Store(key, newEntry)
			return newEntry.timestamp
		}
		return actual.timestamp
	}
	return newEntry.timestamp
}

// getTimingPredicate returns a predicate that stores the first time an object is seen by the
// controller, and cleans up the in-memory map entry when the object is deleted.
func (r *SandboxClaimReconciler) getTimingPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			r.getOrRecordObservedTime(e.Object)
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			r.getOrRecordObservedTime(e.ObjectNew)
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			key := types.NamespacedName{Name: e.Object.GetName(), Namespace: e.Object.GetNamespace()}
			entry, ok := r.observedTimes.Load(key)
			if ok && entry.uid == e.Object.GetUID() {
				r.observedTimes.Delete(key)
			}
			return true
		},
	}
}

// mapWarmPoolToClaims maps a SandboxWarmPool to a list of SandboxClaims that reference it.
func (r *SandboxClaimReconciler) mapWarmPoolToClaims(ctx context.Context, obj client.Object) []ctrl.Request {
	warmPool, ok := obj.(*extensionsv1beta1.SandboxWarmPool)
	if !ok {
		log.FromContext(ctx).Error(fmt.Errorf("unexpected object type %T", obj), "expected SandboxWarmPool in watch map function")
		return nil
	}
	var claims extensionsv1beta1.SandboxClaimList
	if err := r.List(ctx, &claims, client.InNamespace(warmPool.Namespace), client.MatchingFields{extensionsv1beta1.WarmPoolRefField: warmPool.Name}); err != nil {
		log.FromContext(ctx).Error(err, "failed to list SandboxClaims for SandboxWarmPool", "namespace", warmPool.Namespace, "name", warmPool.Name)
		return nil
	}
	var requests []ctrl.Request
	for i := range claims.Items {
		claim := &claims.Items[i]
		// Bound claims are driven by their own Sandbox events; pool churn only
		// matters to claims still waiting (e.g. WarmPoolNotFound).
		if claim.Status.SandboxStatus.Name != "" {
			continue
		}
		requests = append(requests, ctrl.Request{Namespace: claim.Namespace, Name: claim.Name})
	}
	return requests
}

// recordClaimStartupLatency records the startup latency based on webhook annotation.
func (r *SandboxClaimReconciler) recordClaimStartupLatency(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, launchType string, templateName string, warmPoolName string) {
	logger := log.FromContext(ctx)
	webhookSeenTimeStr := claim.Annotations[asmetrics.WebhookAnnotation]
	if webhookSeenTimeStr == "" {
		logger.V(1).Info("Webhook first seen annotation missing, skipping ClaimStartupLatency metric", "claim", claim.Name)
		return
	}
	webhookSeenTime, err := time.Parse(time.RFC3339Nano, webhookSeenTimeStr)
	if err != nil {
		logger.Error(err, "Failed to parse webhook first seen time", "value", webhookSeenTimeStr)
		return
	}
	duration := time.Since(webhookSeenTime)
	if duration < 0 {
		logger.Error(errors.New("negative duration"), "Webhook seen time is in the future", "duration", duration, "webhookSeenTime", webhookSeenTime)
		return
	}
	asmetrics.RecordClaimStartupLatency(webhookSeenTime, launchType, templateName, warmPoolName)
}

// recordControllerStartupLatency records the controller startup latency based on observed time.
func (r *SandboxClaimReconciler) recordControllerStartupLatency(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, launchType string, templateName string, warmPoolName string) {
	logger := log.FromContext(ctx)
	if observedTimeString := claim.Annotations[asmetrics.ObservabilityAnnotation]; observedTimeString != "" {
		key := types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}
		defer r.observedTimes.Delete(key)

		observedTime, err := time.Parse(time.RFC3339Nano, observedTimeString)
		if err != nil {
			logger.Error(err, "Failed to parse controller observation time", "value", observedTimeString)
			return
		}
		asmetrics.RecordClaimControllerStartupLatency(observedTime, launchType, templateName, warmPoolName)
	}
}

func (r *SandboxClaimReconciler) recordSandboxCreationLatency(sandbox *v1beta1.Sandbox, launchType string, templateName string) {
	if sandbox == nil || sandbox.CreationTimestamp.IsZero() {
		return
	}
	sandboxReady := meta.FindStatusCondition(sandbox.Status.Conditions, string(v1beta1.SandboxConditionReady))
	if sandboxReady == nil || sandboxReady.Status != metav1.ConditionTrue || sandboxReady.LastTransitionTime.IsZero() {
		return
	}
	latency := sandboxReady.LastTransitionTime.Sub(sandbox.CreationTimestamp.Time)
	if latency >= 0 {
		asmetrics.RecordSandboxCreationLatency(latency, sandbox.Namespace, launchType, templateName)
	}
}

// recordCreationLatencyMetric detects and records transitions to Ready state.
func (r *SandboxClaimReconciler) recordCreationLatencyMetric(ctx context.Context, claim *extensionsv1beta1.SandboxClaim, oldStatus *extensionsv1beta1.SandboxClaimStatus, sandbox *v1beta1.Sandbox) {
	logger := log.FromContext(ctx)

	newStatus := &claim.Status
	newReady := meta.FindStatusCondition(newStatus.Conditions, string(v1beta1.SandboxConditionReady))
	if newReady == nil || newReady.Status != metav1.ConditionTrue {
		return
	}

	oldReady := meta.FindStatusCondition(oldStatus.Conditions, string(v1beta1.SandboxConditionReady))
	if oldReady != nil && oldReady.Status == metav1.ConditionTrue {
		// Already Ready before this reconcile; drain any entry re-added by a post-Ready UpdateFunc.
		key := types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}
		if entry, ok := r.observedTimes.Load(key); ok && entry.uid == claim.UID {
			r.observedTimes.Delete(key)
		}
		return
	}

	launchType := getLaunchType(sandbox)

	sandboxName := "none"
	if sandbox != nil {
		sandboxName = sandbox.Name
	}

	templateName := r.resolveTemplateName(sandbox)
	warmPoolName := claim.Spec.WarmPoolRef.Name

	logger.V(1).Info("SandboxClaim is marked as Ready", "claim", claim.Name, "sandbox", sandboxName, "duration", time.Since(claim.CreationTimestamp.Time))

	r.recordClaimStartupLatency(ctx, claim, launchType, templateName, warmPoolName)
	r.recordControllerStartupLatency(ctx, claim, launchType, templateName, warmPoolName)
	r.recordSandboxCreationLatency(sandbox, launchType, templateName)
}

// sandboxEventHandler implements handler.EventHandler for the SandboxClaimReconciler.
type sandboxEventHandler struct {
	sandboxQueue *queue.SimpleSandboxQueue
}

func (h *sandboxEventHandler) Create(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.Update(ctx, event.UpdateEvent{ObjectOld: &v1beta1.Sandbox{}, ObjectNew: e.Object}, q)
}

func (h *sandboxEventHandler) Update(ctx context.Context, e event.UpdateEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	newSandbox, ok := e.ObjectNew.(*v1beta1.Sandbox)
	if !ok {
		return
	}
	oldSandbox, ok := e.ObjectOld.(*v1beta1.Sandbox)
	if !ok {
		return
	}

	newAdoptable := isAdoptable(newSandbox) == nil
	oldAdoptable := isAdoptable(oldSandbox) == nil

	logger := log.FromContext(ctx)

	oldWarmPoolName := getWarmPoolName(oldSandbox)
	newWarmPoolName := getWarmPoolName(newSandbox)

	poolChanged := oldWarmPoolName != newWarmPoolName
	nodeScheduled := oldSandbox.Status.NodeName != newSandbox.Status.NodeName

	if (!oldAdoptable && newAdoptable) || (newAdoptable && poolChanged) || (newAdoptable && nodeScheduled) {
		key := queue.SandboxKey{
			Namespace: newSandbox.Namespace,
			Name:      newSandbox.Name,
			NodeName:  newSandbox.Status.NodeName,
		}
		logger.V(1).Info("Adding/updating sandbox in warm pool queue", "warmPool", newWarmPoolName, "namespace", newSandbox.Namespace, "sandbox", key)
		if newWarmPoolName != "" {
			h.sandboxQueue.Add(queue.GetNamespacedWarmPoolName(newSandbox.Namespace, newWarmPoolName), key)
		}
	}
}

func (h *sandboxEventHandler) Generic(_ context.Context, _ event.GenericEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *sandboxEventHandler) Delete(ctx context.Context, e event.DeleteEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	sandbox, ok := e.Object.(*v1beta1.Sandbox)
	if !ok {
		return
	}

	warmPoolName := getWarmPoolName(sandbox)

	if warmPoolName != "" {
		key := queue.SandboxKey{
			Namespace: sandbox.Namespace,
			Name:      sandbox.Name,
		}

		namespacedWarmPoolName := queue.GetNamespacedWarmPoolName(sandbox.Namespace, warmPoolName)

		logger := log.FromContext(ctx)
		logger.V(1).Info("Removing deleted sandbox from warm pool queue", "namespace", sandbox.Namespace, "sandbox", key)
		h.sandboxQueue.RemoveItem(namespacedWarmPoolName, key)
	}
}

type warmPoolEventHandler struct {
	sandboxQueue *queue.SimpleSandboxQueue
}

func (h *warmPoolEventHandler) Create(_ context.Context, _ event.CreateEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *warmPoolEventHandler) Update(_ context.Context, _ event.UpdateEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *warmPoolEventHandler) Generic(_ context.Context, _ event.GenericEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *warmPoolEventHandler) Delete(ctx context.Context, e event.DeleteEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	warmPool, ok := e.Object.(*extensionsv1beta1.SandboxWarmPool)
	if !ok {
		return
	}

	namespacedWarmPoolName := queue.GetNamespacedWarmPoolName(warmPool.Namespace, warmPool.Name)
	logger := log.FromContext(ctx)
	logger.Info("SandboxWarmPool deleted, cleaning up memory queue", "namespace", warmPool.Namespace, "warmPool", warmPool.Name)

	h.sandboxQueue.RemoveQueue(namespacedWarmPoolName)
}

// syncMap is a typed sync.Map.
type syncMap[K comparable, V any] struct {
	inner sync.Map
}

func (m *syncMap[K, V]) Load(key K) (V, bool) {
	val, ok := m.inner.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (m *syncMap[K, V]) Store(key K, entry V) {
	m.inner.Store(key, entry)
}

func (m *syncMap[K, V]) Delete(key K) {
	m.inner.Delete(key)
}

func (m *syncMap[K, V]) LoadOrStore(key K, entry V) (V, bool) {
	actual, loaded := m.inner.LoadOrStore(key, entry)
	return actual.(V), loaded
}

func readyReasonIs(conditions []metav1.Condition, reason string) bool {
	readyCondition := meta.FindStatusCondition(conditions, string(v1beta1.SandboxConditionReady))
	return readyCondition != nil && readyCondition.Reason == reason
}

func verifySandboxCandidate(candidate *v1beta1.Sandbox, claim *extensionsv1beta1.SandboxClaim) error {
	if candidate.Namespace != claim.Namespace {
		return fmt.Errorf("%w: sandbox is in %q, claim is in %q", ErrCrossNamespaceAdoption, candidate.Namespace, claim.Namespace)
	}

	if err := isAdoptable(candidate); err != nil {
		return err
	}

	warmPoolName := getWarmPoolName(candidate)
	if warmPoolName == "" || warmPoolName != claim.Spec.WarmPoolRef.Name {
		return fmt.Errorf("incorrect warm pool, expected %v", claim.Spec.WarmPoolRef.Name)
	}
	return nil
}

func isAdoptable(candidate *v1beta1.Sandbox) error {
	if !candidate.DeletionTimestamp.IsZero() {
		return fmt.Errorf("sandbox is deleted")
	}
	if _, ok := candidate.Labels[warmPoolSandboxLabel]; !ok {
		return fmt.Errorf("sandbox is missing the warm pool sandbox label")
	}
	if _, ok := candidate.Labels[sandboxTemplateRefHash]; !ok {
		return fmt.Errorf("sandbox is missing the sandbox template ref hash label")
	}

	controllerRef := metav1.GetControllerOf(candidate)
	if controllerRef == nil {
		return fmt.Errorf("sandbox %s/%s is unowned and cannot be safely adopted", candidate.Namespace, candidate.Name)
	}
	if controllerRef.APIVersion != extensionsv1beta1.GroupVersion.String() || controllerRef.Kind != warmPoolKind {
		return fmt.Errorf("sandbox %s/%s is not managed by warm pool. Controller: %v", candidate.Namespace, candidate.Name, controllerRef)
	}
	return nil
}

func getWarmPoolName(obj metav1.Object) string {
	if ctrl := metav1.GetControllerOf(obj); ctrl != nil && ctrl.Kind == warmPoolKind {
		return ctrl.Name
	}
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == warmPoolKind {
			return ref.Name
		}
	}
	return ""
}

func shouldSuppressError(err error) bool {
	for _, target := range suppressErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// soonerRequeue returns the earlier of a pending requeue and a proposed delay,
// treating a zero pending requeue as "none scheduled".
func soonerRequeue(pending, proposed time.Duration) time.Duration {
	if pending > 0 && pending < proposed {
		return pending
	}
	return proposed
}

// stagedAnnotationsSurvived reports whether every staged annotation is still on
// the claim after the writes this pass made.
func stagedAnnotationsSurvived(claim *extensionsv1beta1.SandboxClaim, staged map[string]string) bool {
	for k, want := range staged {
		if claim.Annotations[k] != want {
			return false
		}
	}
	return true
}

func pickNodeSpread(keys []queue.SandboxKey) (queue.SandboxKey, bool) {
	if len(keys) == 0 {
		return queue.SandboxKey{}, false
	}
	nodes := make(map[string]nodeSpread, len(keys))
	best := nodeSpread{first: -1}
	for i, key := range keys {
		if key.NodeName == "" {
			continue
		}
		seen := nodes[key.NodeName]
		if seen.count == 0 {
			seen.first = i
		}
		seen.count++
		nodes[key.NodeName] = seen
		if seen.count > best.count || (seen.count == best.count && seen.first < best.first) {
			best = seen
		}
	}
	if best.count == 0 {
		return keys[0], true
	}
	return keys[best.first], true
}

// setOrDeleteLabel forces labels[key] to want, removing the entry when want is
// empty.
func setOrDeleteLabel(labels map[string]string, key, want string) {
	if want == "" {
		delete(labels, key)
		return
	}
	labels[key] = want
}

// notReady builds the Ready=False condition for f.
func notReady(claim *extensionsv1beta1.SandboxClaim, f failure) metav1.Condition {
	return metav1.Condition{
		Type:               string(v1beta1.SandboxConditionReady),
		Status:             metav1.ConditionFalse,
		Reason:             f.reason,
		Message:            f.message,
		ObservedGeneration: claim.Generation,
	}
}

// readyFailure classifies a reconcile error into the reason the claim surfaces.
func readyFailure(claim *extensionsv1beta1.SandboxClaim, err error) failure {
	switch {
	case errors.Is(err, ErrTemplateNotFound):
		return failure{reasonTemplateNotFound, strings.TrimSuffix(err.Error(), ": "+ErrTemplateNotFound.Error())}
	case errors.Is(err, ErrWarmPoolNotFound):
		return failure{"WarmPoolNotFound", fmt.Sprintf("SandboxWarmPool %q not found", claim.Spec.WarmPoolRef.Name)}
	case errors.Is(err, errAdoptionTriggeredRetry):
		return failure{"AdoptionPending", "Warm-pool sandbox adoption triggered; waiting for cache to converge"}
	case errors.Is(err, ErrInvalidMetadata):
		return failure{reasonInvalidMetadata, err.Error()}
	case errors.Is(err, ErrEnvVarsInjectionRejected):
		return failure{reasonEnvVarsInjectionRejected, err.Error()}
	case errors.Is(err, ErrSandboxNotOwned):
		return failure{extensionsv1beta1.ClaimExpiredReason, fmt.Sprintf("Claim expired. %v; deletion skipped.", err)}
	case errors.Is(err, ErrVolumeClaimTemplatesDisallowed),
		errors.Is(err, ErrVolumeClaimTemplatesOverrideForbidden),
		errors.Is(err, ErrVolumeClaimTemplatesInvalid):
		return failure{"VolumeClaimTemplatesError", err.Error()}
	}
	return failure{reasonReconcilerError, "Error seen: " + err.Error()}
}

// ensureClaimIdentityLabels sets SandboxIDLabel (= claim.UID) on the given label map,
// initializing it if nil. Used on both Sandbox.metadata.labels and
// Sandbox.spec.podTemplate.ObjectMeta.Labels so the platform informer can resolve
// sandbox→claim identity from top-level Sandbox events (KEP-0174 only propagates to
// pod template labels, not top-level Sandbox labels).
func ensureClaimIdentityLabels(labels map[string]string, claim *extensionsv1beta1.SandboxClaim) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[extensionsv1beta1.SandboxIDLabel] = string(claim.UID)
	setOrDeleteLabel(labels, v1beta1.CreatedByLabel, claim.Labels[v1beta1.CreatedByLabel])
	return labels
}

// claimSandboxChangePredicate passes the owned-Sandbox transitions a claim
// mirrors: pod IPs, the Ready and Finished conditions, deletion, and the spec or
// label edits the metadata sync repairs. Other status writes (node name, service,
// label selector) would re-reconcile the claim for nothing.
func claimSandboxChangePredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSb, okOld := e.ObjectOld.(*v1beta1.Sandbox)
			newSb, okNew := e.ObjectNew.(*v1beta1.Sandbox)
			if !okOld || !okNew {
				return true
			}
			return oldSb.Generation != newSb.Generation ||
				oldSb.DeletionTimestamp.IsZero() != newSb.DeletionTimestamp.IsZero() ||
				!maps.Equal(oldSb.Labels, newSb.Labels) ||
				!slices.Equal(oldSb.Status.PodIPs, newSb.Status.PodIPs) ||
				sandboxConditionChanged(oldSb, newSb, string(v1beta1.SandboxConditionReady)) ||
				sandboxConditionChanged(oldSb, newSb, string(v1beta1.SandboxConditionFinished))
		},
	}
}

func sandboxConditionChanged(oldSb, newSb *v1beta1.Sandbox, conditionType string) bool {
	oldCond := meta.FindStatusCondition(oldSb.Status.Conditions, conditionType)
	newCond := meta.FindStatusCondition(newSb.Status.Conditions, conditionType)
	if oldCond == nil || newCond == nil {
		return oldCond != newCond
	}
	return *oldCond != *newCond
}

// isSandboxReady checks if a sandbox has Ready=True condition.
func isSandboxReady(sb *v1beta1.Sandbox) bool {
	for _, cond := range sb.Status.Conditions {
		if cond.Type == string(v1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func isRestrictedDomain(domain string) bool {
	return domainInList(domain, restrictedDomains)
}

func domainInList(domain string, list []string) bool {
	return slices.ContainsFunc(list, func(d string) bool {
		return domain == d || strings.HasSuffix(domain, "."+d)
	})
}

func mergeVolumeClaimTemplates(templateVCTs, claimVCTs []v1beta1.PersistentVolumeClaimTemplate, policy extensionsv1beta1.VolumeClaimTemplatesPolicy) ([]v1beta1.PersistentVolumeClaimTemplate, error) {
	if err := validateVolumeClaimTemplates(templateVCTs); err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}

	if len(claimVCTs) == 0 {
		return templateVCTs, nil
	}

	if err := validateVolumeClaimTemplates(claimVCTs); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}

	switch policy {
	case extensionsv1beta1.VolumeClaimTemplatesPolicyDisallowed, "":
		return nil, ErrVolumeClaimTemplatesDisallowed

	case extensionsv1beta1.VolumeClaimTemplatesPolicyAllowed:
		templateMap := make(map[string]struct{}, len(templateVCTs))
		for _, vct := range templateVCTs {
			templateMap[vct.Name] = struct{}{}
		}
		for _, vct := range claimVCTs {
			if _, exists := templateMap[vct.Name]; exists {
				return nil, fmt.Errorf("%w: cannot override template volume %q", ErrVolumeClaimTemplatesOverrideForbidden, vct.Name)
			}
		}
		return slices.Concat(templateVCTs, claimVCTs), nil

	case extensionsv1beta1.VolumeClaimTemplatesPolicyOverrides:
		merged := make([]v1beta1.PersistentVolumeClaimTemplate, 0, len(templateVCTs)+len(claimVCTs))
		claimMap := make(map[string]v1beta1.PersistentVolumeClaimTemplate, len(claimVCTs))
		for _, vct := range claimVCTs {
			claimMap[vct.Name] = vct
		}

		for _, vct := range templateVCTs {
			if override, ok := claimMap[vct.Name]; ok {
				merged = append(merged, override)
				delete(claimMap, vct.Name)
			} else {
				merged = append(merged, vct)
			}
		}

		for _, vct := range claimVCTs {
			if _, exists := claimMap[vct.Name]; exists {
				merged = append(merged, vct)
			}
		}
		return merged, nil

	default:
		return nil, fmt.Errorf("unknown volume claim templates policy %q", policy)
	}
}

func validateVolumeClaimTemplates(vcts []v1beta1.PersistentVolumeClaimTemplate) error {
	names := make(map[string]struct{}, len(vcts))
	for i, vct := range vcts {
		if vct.Name == "" {
			return fmt.Errorf("%w: name at index %d is empty", ErrVolumeClaimTemplatesInvalid, i)
		}
		if _, exists := names[vct.Name]; exists {
			return fmt.Errorf("%w: duplicate name %q", ErrVolumeClaimTemplatesInvalid, vct.Name)
		}
		names[vct.Name] = struct{}{}
	}
	return nil
}

// warmPoolRefIndexer indexes SandboxClaims by spec.warmPoolRef.name.
func warmPoolRefIndexer(rawObj client.Object) []string {
	claim, ok := rawObj.(*extensionsv1beta1.SandboxClaim)
	if !ok || claim.Spec.WarmPoolRef.Name == "" {
		return nil
	}
	return []string{claim.Spec.WarmPoolRef.Name}
}

// getLaunchType determines the launch type based on the sandbox state.
func getLaunchType(sandbox *v1beta1.Sandbox) string {
	if sandbox == nil {
		return asmetrics.LaunchTypeUnknown
	}
	if sandbox.Labels[v1beta1.SandboxLaunchTypeLabel] == v1beta1.SandboxLaunchTypeWarm {
		return asmetrics.LaunchTypeWarm
	}
	return asmetrics.LaunchTypeCold
}
