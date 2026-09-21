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
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	sandboxv1alpha1 "github.com/cocoonstack/sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
)

const (
	resourceOwnedBySandbox resourceOwnership = iota
	resourceUnowned
	resourceOwnedByOther

	warmPoolKind = "SandboxWarmPool"

	msgPodSucceeded   = "Pod completed successfully"
	msgPodFailed      = "Pod failed"
	msgSandboxExpired = "Sandbox has expired"

	sandboxLabel                = "agents.x-k8s.io/sandbox-name-hash"
	sandboxControllerFieldOwner = "sandbox-controller"
	immediateRequeueDelay       = time.Millisecond
)

// Scheme registers the types sandbox controllers need on their client.
var Scheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(sandboxv1alpha1.AddToScheme(s))
	utilruntime.Must(sandboxv1beta1.AddToScheme(s))
	return s
}()

// resourceOwnership represents the ownership state of a Kubernetes resource relative to a Sandbox.
type resourceOwnership int

// PodMutator applies runtime-specific defaults to a newly-created Sandbox Pod.
// Implementations must preserve every field explicitly supplied through the
// Sandbox API and return an error instead of silently overriding conflicts.
type PodMutator interface {
	MutatePod(context.Context, *sandboxv1beta1.Sandbox, *corev1.Pod) error
}

//+kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes/finalizers,verbs=get;update;patch
//+kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;update;patch,resourceNames=sandboxes.agents.x-k8s.io;sandboxclaims.extensions.agents.x-k8s.io;sandboxtemplates.extensions.agents.x-k8s.io;sandboxwarmpools.extensions.agents.x-k8s.io

// SandboxReconciler reconciles a Sandbox object.
type SandboxReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Tracer        asmetrics.Instrumenter
	ClusterDomain string
	PodMutator    PodMutator
}

// Reconcile drives a Sandbox toward its declared spec: child Service/Pod/PVCs,
// status conditions, and expiry.
func (r *SandboxReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	sandbox := &sandboxv1beta1.Sandbox{}
	if err := r.Get(ctx, req.NamespacedName, sandbox); err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("sandbox resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	initialAttrs := map[string]string{
		"sandbox.name":      sandbox.Name,
		"sandbox.namespace": sandbox.Namespace,
	}
	if val, ok := sandbox.Labels[sandboxv1beta1.CreatedByLabel]; ok {
		initialAttrs[sandboxv1beta1.CreatedByLabel] = asmetrics.NormalizeCreatedBy(val)
	}
	ctx, end := r.Tracer.StartSpan(ctx, sandbox, "ReconcileSandbox", initialAttrs)
	defer end()

	if !sandbox.DeletionTimestamp.IsZero() {
		logger.Info("Sandbox is being deleted")
		return ctrl.Result{}, nil
	}

	tc := r.Tracer.GetTraceContext(ctx)
	if tc != "" && (sandbox.Annotations == nil || sandbox.Annotations[asmetrics.TraceContextAnnotation] == "") {
		patch := client.MergeFrom(sandbox.DeepCopy())
		if sandbox.Annotations == nil {
			sandbox.Annotations = make(map[string]string)
		}
		sandbox.Annotations[asmetrics.TraceContextAnnotation] = tc

		if err := r.Patch(ctx, sandbox, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	oldStatus := sandbox.Status.DeepCopy()
	var err error
	sandboxDeleted := false
	result := ctrl.Result{}

	expired, _ := checkSandboxExpiry(sandbox, time.Now())
	switch {
	case expired && !sandboxMarkedExpired(sandbox):
		setSandboxExpiredCondition(sandbox)
		if statusUpdateErr := r.updateStatus(ctx, oldStatus, sandbox); statusUpdateErr != nil {
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{RequeueAfter: immediateRequeueDelay}, nil
	case expired:
		logger.Info("Sandbox has expired, deleting child resources and checking shutdown policy")
		sandboxDeleted, err = r.handleSandboxExpiry(ctx, sandbox)
	default:
		err = r.reconcileChildResources(ctx, sandbox)
		expiredAfterReconcile, requeueAfter := checkSandboxExpiry(sandbox, time.Now())
		result.RequeueAfter = requeueAfter
		if expiredAfterReconcile {
			setSandboxExpiredCondition(sandbox)
			result.RequeueAfter = immediateRequeueDelay
		}
	}

	if !sandboxDeleted {
		if statusUpdateErr := r.updateStatus(ctx, oldStatus, sandbox); statusUpdateErr != nil {
			err = errors.Join(err, statusUpdateErr)
		}
	}
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *SandboxReconciler) SetupWithManager(mgr ctrl.Manager, concurrentWorkers int) error {
	labelSelectorPredicate, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      sandboxLabel,
				Operator: metav1.LabelSelectorOpExists,
				Values:   []string{},
			},
		},
	})
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&sandboxv1beta1.Sandbox{}).
		Owns(&corev1.Pod{}, builder.WithPredicates(labelSelectorPredicate)).
		Owns(&corev1.Service{}, builder.WithPredicates(labelSelectorPredicate)).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentWorkers}).
		Complete(r)
}

func (r *SandboxReconciler) reconcileChildResources(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) error {
	nameHash := hash.Name(sandbox.Name)

	var allErrors error

	err := r.reconcilePVCs(ctx, sandbox, nameHash)
	allErrors = errors.Join(allErrors, err)

	pod, err := r.reconcilePod(ctx, sandbox, nameHash)
	allErrors = errors.Join(allErrors, err)
	if pod == nil {
		sandbox.Status.PodIPs = nil
		sandbox.Status.NodeName = ""
	} else {
		sandbox.Status.LabelSelector = fmt.Sprintf("%s=%s", sandboxLabel, hash.Name(sandbox.Name))
		sandbox.Status.PodIPs = podIPsFromStatus(pod.Status.PodIPs)
		sandbox.Status.NodeName = pod.Spec.NodeName
	}

	svc, err := r.reconcileService(ctx, sandbox, nameHash)
	allErrors = errors.Join(allErrors, err)

	conditions := r.computeConditions(sandbox, allErrors, svc, pod)
	hasFinished := false
	for _, condition := range conditions {
		meta.SetStatusCondition(&sandbox.Status.Conditions, condition)
		if condition.Type == string(sandboxv1beta1.SandboxConditionFinished) {
			hasFinished = true
		}
	}

	sandbox.Status.Conditions = slices.DeleteFunc(sandbox.Status.Conditions, func(condition metav1.Condition) bool {
		return condition.Type == string(sandboxv1beta1.SandboxConditionFinished) && !hasFinished ||
			condition.Type == string(sandboxv1beta1.SandboxConditionSuspended) && sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended
	})

	return allErrors
}

func (r *SandboxReconciler) computeConditions(sandbox *sandboxv1beta1.Sandbox, err error, svc *corev1.Service, pod *corev1.Pod) []metav1.Condition {
	var conditions []metav1.Condition

	if suspended := r.computeSuspendedCondition(sandbox, pod); suspended != nil {
		conditions = append(conditions, *suspended)
	}

	if finished := r.computeFinishedCondition(sandbox, pod); finished != nil {
		conditions = append(conditions, *finished)
	}

	conditions = append(conditions, r.computeReadyCondition(sandbox, err, svc, pod))

	return conditions
}

func (r *SandboxReconciler) computeSuspendedCondition(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) *metav1.Condition {
	isSuspended := sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended
	if !isSuspended {
		return nil
	}

	suspended := metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionSuspended),
		ObservedGeneration: sandbox.Generation,
	}
	if pod == nil {
		suspended.Status = metav1.ConditionTrue
		suspended.Reason = sandboxv1beta1.SandboxReasonSuspendedPodTerminated
		suspended.Message = "Pod has been terminated. Sandbox is not operational."
	} else {
		suspended.Status = metav1.ConditionFalse
		suspended.Reason = sandboxv1beta1.SandboxReasonSuspendedPodNotTerminated
		suspended.Message = "Pod has not been terminated. Sandbox is operational."
	}

	return &suspended
}

func (r *SandboxReconciler) computeReadyCondition(sandbox *sandboxv1beta1.Sandbox, err error, svc *corev1.Service, pod *corev1.Pod) metav1.Condition {
	readyCondition := metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionReady),
		ObservedGeneration: sandbox.Generation,
		Message:            "",
		Status:             metav1.ConditionFalse,
		Reason:             sandboxv1beta1.SandboxReasonDependenciesNotReady,
	}

	if err != nil {
		readyCondition.Reason = "ReconcilerError"
		readyCondition.Message = "Error seen: " + err.Error()
		return readyCondition
	}

	isSuspended := sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended
	if isSuspended {
		readyCondition.Reason = sandboxv1beta1.SandboxReasonSuspended
		if pod != nil {
			readyCondition.Message = "Sandbox is suspending"
		} else {
			readyCondition.Message = "Sandbox is suspended"
		}
		return readyCondition
	}

	if pod != nil {
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			readyCondition.Reason = sandboxv1beta1.SandboxReasonPodSucceeded
			readyCondition.Message = msgPodSucceeded
			return readyCondition
		case corev1.PodFailed:
			readyCondition.Reason = sandboxv1beta1.SandboxReasonPodFailed
			readyCondition.Message = msgPodFailed
			return readyCondition
		}
	}

	message, podReady := podReadiness(pod)

	// svcRequired: true if the sandbox explicitly requests a service or if a
	// service already exists.
	svcRequired := false
	if sandbox.Spec.Service != nil {
		svcRequired = *sandbox.Spec.Service
	} else if svc != nil {
		// Backward compatibility: require service readiness
		svcRequired = true
	}

	svcReady := true
	if svcRequired {
		svcReady = false
		if svc != nil {
			message += "; Service Exists"
			svcReady = true
		} else {
			message += "; Service does not exist"
		}
	}

	readyCondition.Message = message
	if podReady && svcReady {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = sandboxv1beta1.SandboxReasonDependenciesReady
	}

	return readyCondition
}

func (r *SandboxReconciler) computeFinishedCondition(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) *metav1.Condition {
	if pod == nil {
		return nil
	}

	condition := &metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionFinished),
		Status:             metav1.ConditionTrue,
		ObservedGeneration: sandbox.Generation,
	}

	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		condition.Reason = sandboxv1beta1.SandboxReasonPodSucceeded
		condition.Message = msgPodSucceeded
	case corev1.PodFailed:
		condition.Reason = sandboxv1beta1.SandboxReasonPodFailed
		condition.Message = msgPodFailed
	default:
		return nil
	}

	return condition
}

func (r *SandboxReconciler) updateStatus(ctx context.Context, oldStatus *sandboxv1beta1.SandboxStatus, sandbox *sandboxv1beta1.Sandbox) error {
	logger := log.FromContext(ctx)

	if reflect.DeepEqual(oldStatus, &sandbox.Status) {
		return nil
	}

	if err := r.Status().Update(ctx, sandbox); err != nil {
		logger.Error(err, "Failed to update sandbox status")
		return err
	}

	return nil
}

func (r *SandboxReconciler) reconcileService(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, nameHash string) (*corev1.Service, error) {
	logger := log.FromContext(ctx)
	desired := sandbox.Spec.Service

	service := &corev1.Service{}
	getErr := r.Get(ctx, types.NamespacedName{Name: sandbox.Name, Namespace: sandbox.Namespace}, service)
	if getErr != nil && !k8serrors.IsNotFound(getErr) {
		logger.Error(getErr, "Failed to get Service")
		return nil, fmt.Errorf("service get failed: %w", getErr)
	}
	if getErr != nil {
		if desired == nil || !*desired {
			r.clearServiceStatus(sandbox)
			return nil, nil
		}
		return r.createHeadlessService(ctx, sandbox, nameHash)
	}

	logger.Info("Found Service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)

	ownership, controllerRef := checkOwnership(service, sandbox)

	if desired != nil && !*desired {
		if ownership == resourceOwnedBySandbox {
			logger.Info("Deleting owned service because service is disabled",
				"Service.Name", service.Name, "Sandbox.Name", sandbox.Name)
			if err := r.Delete(ctx, service); err != nil && !k8serrors.IsNotFound(err) {
				return nil, fmt.Errorf("delete service: %w", err)
			}
		}
		r.clearServiceStatus(sandbox)
		return nil, nil
	}

	switch ownership {
	case resourceOwnedByOther:
		logger.Info("Refusing to use service: service is owned by a different controller",
			"Service.Name", service.Name, "Sandbox.Name", sandbox.Name,
			"Owner.Kind", controllerRef.Kind, "Owner.Name", controllerRef.Name, "Owner.UID", controllerRef.UID)
		return nil, fmt.Errorf("service %q is owned by %s/%s (UID: %s), not by sandbox %q",
			service.Name, controllerRef.Kind, controllerRef.Name, controllerRef.UID, sandbox.Name)

	case resourceUnowned:
		if desired == nil {
			r.clearServiceStatus(sandbox)
			return nil, nil
		}
		isAdoptablePool := isAdoptable(service)
		hasTrackingLabel := service.Labels != nil && service.Labels[sandboxLabel] == nameHash
		if !isAdoptablePool && !hasTrackingLabel {
			logger.V(4).Info("Refusing to adopt unowned service: missing pool authorization label or sandbox tracking label",
				"Service.Name", service.Name, "Sandbox.Name", sandbox.Name,
				"RequiredLabel", sandboxv1beta1.SandboxAdoptableLabel, "TrackingLabel", sandboxLabel)
			return nil, fmt.Errorf("cannot adopt unowned service %q: missing required pool authorization label (%q) or sandbox tracking label (%q)",
				service.Name, sandboxv1beta1.SandboxAdoptableLabel, sandboxLabel)
		}
		if service.Spec.ClusterIP != corev1.ClusterIPNone && service.Spec.ClusterIP != "" {
			logger.V(4).Info("Refusing to adopt service: ClusterIP mismatch (immutable, expected None)",
				"Service.Name", service.Name, "Sandbox.Name", sandbox.Name,
				"Service.ClusterIP", service.Spec.ClusterIP)
			return nil, fmt.Errorf("cannot adopt service %q: ClusterIP is %q (expected %q, field is immutable)",
				service.Name, service.Spec.ClusterIP, corev1.ClusterIPNone)
		}

		logger.Info("Adopting unowned service", "Service.Name", service.Name, "Sandbox.Name", sandbox.Name)

		if service.Labels == nil {
			service.Labels = make(map[string]string)
		}
		service.Labels[sandboxLabel] = nameHash
		service.Spec.Selector = map[string]string{
			sandboxLabel: nameHash,
		}

		if err := ctrl.SetControllerReference(sandbox, service, r.Scheme); err != nil {
			return nil, fmt.Errorf("SetControllerReference for Service failed: %w", err)
		}
		if err := r.Update(ctx, service); err != nil {
			return nil, fmt.Errorf("update service with owner reference: %w", err)
		}

	case resourceOwnedBySandbox:
		desiredSelector := map[string]string{
			sandboxLabel: nameHash,
		}
		patch := client.MergeFrom(service.DeepCopy())
		needsUpdate := false

		if service.Labels == nil {
			service.Labels = make(map[string]string)
		}
		if service.Labels[sandboxLabel] != nameHash {
			service.Labels[sandboxLabel] = nameHash
			needsUpdate = true
		}
		if !reflect.DeepEqual(service.Spec.Selector, desiredSelector) {
			service.Spec.Selector = desiredSelector
			needsUpdate = true
		}

		if needsUpdate {
			logger.Info("Reconciling owned service drift", "Service.Namespace", service.Namespace, "Service.Name", service.Name, "Sandbox.Namespace", sandbox.Namespace, "Sandbox.Name", sandbox.Name)
			if err := r.Patch(ctx, service, patch); err != nil {
				return nil, fmt.Errorf("patch owned service: %w", err)
			}
		}
	}

	r.setServiceStatus(sandbox, service)
	return service, nil
}

func (r *SandboxReconciler) clearPodNameAnnotation(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) error {
	if _, exists := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; !exists {
		return nil
	}
	logger := log.FromContext(ctx)
	patch := client.MergeFrom(sandbox.DeepCopy())
	delete(sandbox.Annotations, sandboxv1beta1.SandboxPodNameAnnotation)
	if err := r.Patch(ctx, sandbox, patch); err != nil {
		return fmt.Errorf("clear pod name annotation: %w", err)
	}
	logger.Info("Removed pod name annotation from sandbox", "Sandbox.Name", sandbox.Name)
	return nil
}

// createHeadlessService creates the selector-only Service fronting the Sandbox pod.
func (r *SandboxReconciler) createHeadlessService(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, nameHash string) (*corev1.Service, error) {
	logger := log.FromContext(ctx)
	logger.Info("Creating a new Headless Service", "Service.Namespace", sandbox.Namespace, "Service.Name", sandbox.Name)
	service := &corev1.Service{
		Name:      sandbox.Name,
		Namespace: sandbox.Namespace,
		Labels:    map[string]string{sandboxLabel: nameHash},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  map[string]string{sandboxLabel: nameHash},
		},
	}
	service.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := ctrl.SetControllerReference(sandbox, service, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference")
		return nil, fmt.Errorf("SetControllerReference for Service failed: %w", err)
	}
	if err := r.Create(ctx, service, client.FieldOwner(sandboxControllerFieldOwner)); err != nil {
		logger.Error(err, "Failed to create", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
		return nil, err
	}
	r.setServiceStatus(sandbox, service)
	return service, nil
}

// setServiceStatus updates the sandbox status with the service name and FQDN.
func (r *SandboxReconciler) setServiceStatus(sandbox *sandboxv1beta1.Sandbox, service *corev1.Service) {
	sandbox.Status.Service = service.Name
	sandbox.Status.ServiceFQDN = service.Name + "." + service.Namespace + ".svc." + r.ClusterDomain
}

// clearServiceStatus clears the service-related fields from sandbox status.
func (r *SandboxReconciler) clearServiceStatus(sandbox *sandboxv1beta1.Sandbox) {
	sandbox.Status.Service = ""
	sandbox.Status.ServiceFQDN = ""
}

func (r *SandboxReconciler) reconcilePod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, nameHash string) (*corev1.Pod, error) {
	ctx, end := r.Tracer.StartSpan(ctx, nil, "reconcilePod", nil)
	defer end()

	pod, err := r.lookupTrackedPod(ctx, sandbox)
	if err != nil {
		return nil, err
	}

	if sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
		return nil, r.suspendPod(ctx, sandbox, pod)
	}
	if pod != nil {
		return r.adoptPod(ctx, sandbox, pod, nameHash)
	}
	return r.createPod(ctx, sandbox, nameHash)
}

// lookupTrackedPod resolves the Sandbox's pod, returning nil when it is absent.
// A dangling pod-name annotation is cleared so the next pass can create afresh.
func (r *SandboxReconciler) lookupTrackedPod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (*corev1.Pod, error) {
	logger := log.FromContext(ctx)
	podName := resolvePodName(sandbox)
	_, annotated := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]
	if podName != sandbox.Name {
		logger.Info("Using tracked pod name from sandbox annotation", "podName", podName)
	}

	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: sandbox.Namespace}, pod)
	if err == nil {
		return pod, nil
	}
	if !k8serrors.IsNotFound(err) {
		logger.Error(err, "Failed to get Pod")
		return nil, fmt.Errorf("pod get failed: %w", err)
	}
	if annotated {
		logger.Info("Pod referenced by annotation not found, clearing annotation to recover state", "podName", podName)
		if err := r.clearPodNameAnnotation(ctx, sandbox); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// suspendPod deletes the Sandbox's own pod and drops the tracking annotation.
// A pod this Sandbox does not control is left alone.
func (r *SandboxReconciler) suspendPod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	logger := log.FromContext(ctx)
	if pod != nil {
		switch ownership, controllerRef := checkOwnership(pod, sandbox); ownership {
		case resourceOwnedBySandbox:
			if !pod.DeletionTimestamp.IsZero() {
				logger.Info("Pod is already being deleted", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
				break
			}
			logger.Info("Deleting Pod because .Spec.OperatingMode is Suspended", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
			if err := r.Delete(ctx, pod); err != nil {
				return fmt.Errorf("delete pod: %w", err)
			}
		case resourceUnowned:
			logger.Info("Refusing to delete pod: pod has no controllerRef pointing to this sandbox",
				"Pod.Name", pod.Name, "Sandbox.Name", sandbox.Name)
		case resourceOwnedByOther:
			logger.Info("Refusing to delete pod: pod is owned by a different controller",
				"Pod.Name", pod.Name, "Sandbox.Name", sandbox.Name,
				"Owner.Kind", controllerRef.Kind, "Owner.Name", controllerRef.Name, "Owner.UID", controllerRef.UID)
		}
	}
	return r.clearPodNameAnnotation(ctx, sandbox)
}

// adoptPod brings an existing pod (warm-pool hand-off, or a survivor from an
// earlier pass) under this Sandbox and syncs its metadata.
func (r *SandboxReconciler) adoptPod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, nameHash string) (*corev1.Pod, error) {
	logger := log.FromContext(ctx)
	logger.Info("Found Pod", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
	if r.Tracer.IsRecording(ctx) {
		r.Tracer.AddEvent(ctx, "ExistingPodStatusObserved", map[string]string{
			"pod.Name":  pod.Name,
			"pod.Phase": string(pod.Status.Phase),
		})
	}

	patch := client.MergeFrom(pod.DeepCopy())
	needsUpdate := false
	switch ownership, controllerRef := checkOwnership(pod, sandbox); ownership {
	case resourceOwnedByOther:
		logger.V(4).Info("Refusing to adopt pod: pod is owned by a different controller",
			"Pod.Name", pod.Name, "Sandbox.Name", sandbox.Name,
			"Owner.Kind", controllerRef.Kind, "Owner.Name", controllerRef.Name, "Owner.UID", controllerRef.UID)
		if err := r.clearPodNameAnnotation(ctx, sandbox); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("pod %q is owned by %s/%s (UID: %s), not by sandbox %q",
			pod.Name, controllerRef.Kind, controllerRef.Name, controllerRef.UID, sandbox.Name)

	case resourceUnowned:
		hasTrackingLabel := pod.Labels[sandboxLabel] == nameHash
		if !isAdoptable(pod) && !hasTrackingLabel {
			logger.V(4).Info("Refusing to adopt unowned pod: missing pool authorization label or sandbox tracking label",
				"Pod.Name", pod.Name, "Sandbox.Name", sandbox.Name,
				"RequiredLabel", sandboxv1beta1.SandboxAdoptableLabel, "TrackingLabel", sandboxLabel)
			return nil, fmt.Errorf("cannot adopt unowned pod %q: missing required pool authorization label (%q) or sandbox tracking label (%q)",
				pod.Name, sandboxv1beta1.SandboxAdoptableLabel, sandboxLabel)
		}
		if err := ctrl.SetControllerReference(sandbox, pod, r.Scheme); err != nil {
			return nil, fmt.Errorf("SetControllerReference for Pod failed: %w", err)
		}
		needsUpdate = true

	case resourceOwnedBySandbox:
	}

	if r.updatePodMetadata(ctx, pod, sandbox, nameHash) || needsUpdate {
		if err := r.Patch(ctx, pod, patch); err != nil {
			return nil, fmt.Errorf("patch pod: %w", err)
		}
	}
	if err := r.ensurePodNameAnnotation(ctx, sandbox, pod.Name); err != nil {
		return nil, err
	}
	return pod, nil
}

// createPod builds the Sandbox's pod from its template. Losing the create race
// means another pass already made it, so the winner is adopted instead.
func (r *SandboxReconciler) createPod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, nameHash string) (*corev1.Pod, error) {
	logger := log.FromContext(ctx)
	logger.Info("Creating a new Pod", "Pod.Namespace", sandbox.Namespace, "Pod.Name", sandbox.Name)

	tmpl := sandbox.Spec.PodTemplate.ObjectMeta
	podLabels, managedLabelKeys := filterSystemKeys(tmpl.Labels, isSystemLabel, func(k string) {
		logger.V(1).Info("Ignoring system-reserved label in Sandbox PodTemplate", "key", k)
	})
	// System-owned labels are assigned after the template merge so it cannot override them.
	podLabels[sandboxLabel] = nameHash
	warmPoolHash, templateRefHash := extensionOwnedHashes(sandbox)
	if warmPoolHash != "" {
		podLabels[sandboxv1beta1.SandboxWarmPoolLabel] = warmPoolHash
	}
	if templateRefHash != "" {
		podLabels[sandboxv1beta1.SandboxTemplateRefHashLabel] = templateRefHash
	}
	if val := sandbox.Labels[sandboxv1beta1.CreatedByLabel]; val != "" {
		podLabels[sandboxv1beta1.CreatedByLabel] = asmetrics.NormalizeCreatedBy(val)
	}

	annotations, managedAnnotationKeys := filterSystemKeys(tmpl.Annotations, isSystemAnnotation, func(k string) {
		logger.V(1).Info("Ignoring system-reserved annotation in Sandbox PodTemplate", "key", k)
	})
	slices.Sort(managedLabelKeys)
	slices.Sort(managedAnnotationKeys)
	if len(managedLabelKeys) > 0 {
		annotations[sandboxv1beta1.SandboxPropagatedLabelsAnnotation] = strings.Join(managedLabelKeys, ",")
	}
	if len(managedAnnotationKeys) > 0 {
		annotations[sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation] = strings.Join(managedAnnotationKeys, ",")
	}

	mutatedSpec := sandbox.Spec.PodTemplate.Spec.DeepCopy()
	pvcVolumes := make([]corev1.Volume, 0, len(sandbox.Spec.VolumeClaimTemplates))
	for _, pvcTemplate := range sandbox.Spec.VolumeClaimTemplates {
		pvcVolumes = append(pvcVolumes, corev1.Volume{
			Name: pvcTemplate.Name,
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcTemplate.Name + "-" + sandbox.Name,
			},
		})
	}
	mutatedSpec.Volumes = MergeVolumeClaimVolumes(mutatedSpec.Volumes, pvcVolumes)

	pod := &corev1.Pod{
		Name:        sandbox.Name,
		Namespace:   sandbox.Namespace,
		Labels:      podLabels,
		Annotations: annotations,
		Spec:        *mutatedSpec,
	}
	if r.PodMutator != nil {
		if err := r.PodMutator.MutatePod(ctx, sandbox, pod); err != nil {
			return nil, fmt.Errorf("mutate sandbox pod: %w", err)
		}
	}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	if err := ctrl.SetControllerReference(sandbox, pod, r.Scheme); err != nil {
		return nil, fmt.Errorf("SetControllerReference for Pod failed: %w", err)
	}

	if err := r.Create(ctx, pod, client.FieldOwner(sandboxControllerFieldOwner)); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
			return nil, err
		}
		logger.Info("Pod already exists, fetching existing pod", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
		existing := &corev1.Pod{}
		if getErr := r.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, existing); getErr != nil {
			return nil, fmt.Errorf("fetch existing pod: %w", getErr)
		}
		return r.adoptPod(ctx, sandbox, existing, nameHash)
	}

	if err := r.ensurePodNameAnnotation(ctx, sandbox, pod.Name); err != nil {
		return nil, err
	}
	if r.Tracer.IsRecording(ctx) {
		r.Tracer.AddEvent(ctx, "NewPodStatusObserved", map[string]string{
			"pod.Name":  pod.Name,
			"pod.Phase": string(pod.Status.Phase),
		})
	}
	return pod, nil
}

// ensurePodNameAnnotation records which pod this Sandbox owns. A Sandbox that
// already tracks a different pod is left alone rather than retargeted.
func (r *SandboxReconciler) ensurePodNameAnnotation(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, podName string) error {
	tracked := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]
	if tracked == podName {
		return nil
	}
	if tracked != "" {
		log.FromContext(ctx).Info("Skipping pod name annotation update because sandbox already tracks a different pod",
			"trackedPodName", tracked, "podName", podName)
		return nil
	}
	patch := client.MergeFrom(sandbox.DeepCopy())
	if sandbox.Annotations == nil {
		sandbox.Annotations = make(map[string]string)
	}
	sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation] = podName
	if err := r.Patch(ctx, sandbox, patch); err != nil {
		return fmt.Errorf("set pod name annotation: %w", err)
	}
	return nil
}

func (r *SandboxReconciler) updatePodMetadata(ctx context.Context, pod *corev1.Pod, sandbox *sandboxv1beta1.Sandbox, nameHash string) bool {
	logger := log.FromContext(ctx)
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	updated := setOrDelete(pod.Labels, sandboxLabel, nameHash)

	tmpl := sandbox.Spec.PodTemplate.ObjectMeta
	managedLabels, labelsUpdated := propagateKeys(pod.Labels, tmpl.Labels, isSystemLabel, func(k string) {
		logger.V(1).Info("Ignoring system-reserved label in Sandbox PodTemplate", "pod", pod.Name, "key", k)
	})
	updated = labelsUpdated || updated
	updated = prunePropagated(pod.Labels, pod.Annotations[sandboxv1beta1.SandboxPropagatedLabelsAnnotation],
		tmpl.Labels, isSystemLabel,
		func(k string) bool { return k == sandboxLabel },
		func(k string) {
			logger.V(1).Info("Removed unauthorized system label from Pod", "pod", pod.Name, "key", k)
		}) || updated

	warmPoolHash, templateRefHash := extensionOwnedHashes(sandbox)
	updated = setOrDelete(pod.Labels, sandboxv1beta1.SandboxWarmPoolLabel, warmPoolHash) || updated
	updated = setOrDelete(pod.Labels, sandboxv1beta1.SandboxTemplateRefHashLabel, templateRefHash) || updated

	// The created-by value is normalized to the known allow-list so an arbitrary
	// Sandbox label cannot put unbounded cardinality on the Pod.
	var createdBy string
	if val := sandbox.Labels[sandboxv1beta1.CreatedByLabel]; val != "" {
		createdBy = asmetrics.NormalizeCreatedBy(val)
	}
	updated = setOrDelete(pod.Labels, sandboxv1beta1.CreatedByLabel, createdBy) || updated

	managedAnnotations, annotationsUpdated := propagateKeys(pod.Annotations, tmpl.Annotations, isSystemAnnotation, func(k string) {
		logger.V(1).Info("Ignoring system-reserved annotation in Sandbox PodTemplate", "pod", pod.Name, "key", k)
	})
	updated = annotationsUpdated || updated
	updated = prunePropagated(pod.Annotations, pod.Annotations[sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation],
		tmpl.Annotations, isSystemAnnotation, isControllerManagedPodAnnotation, func(string) {}) || updated

	slices.Sort(managedLabels)
	slices.Sort(managedAnnotations)
	updated = setEntry(pod.Annotations, sandboxv1beta1.SandboxPropagatedLabelsAnnotation, strings.Join(managedLabels, ",")) || updated
	updated = setEntry(pod.Annotations, sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation, strings.Join(managedAnnotations, ",")) || updated
	return updated
}

func (r *SandboxReconciler) reconcilePVCs(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, nameHash string) error {
	logger := log.FromContext(ctx)

	ctx, end := r.Tracer.StartSpan(ctx, nil, "reconcilePVCs", nil)
	defer end()

	for _, pvcTemplate := range sandbox.Spec.VolumeClaimTemplates {
		pvc := &corev1.PersistentVolumeClaim{}
		pvcName := pvcTemplate.Name + "-" + sandbox.Name
		getErr := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sandbox.Namespace}, pvc)
		if getErr == nil {
			ownership, controllerRef := checkOwnership(pvc, sandbox)
			switch ownership {
			case resourceOwnedByOther:
				logger.V(4).Info("Refusing to use PVC: PVC is owned by a different controller",
					"PVC.Name", pvcName, "Sandbox.Name", sandbox.Name,
					"Owner.Kind", controllerRef.Kind, "Owner.Name", controllerRef.Name, "Owner.UID", controllerRef.UID)
				return fmt.Errorf("PVC %q is owned by %s/%s (UID: %s), not by sandbox %q",
					pvcName, controllerRef.Kind, controllerRef.Name, controllerRef.UID, sandbox.Name)

			case resourceUnowned:
				isAdoptablePool := isAdoptable(pvc)
				hasTrackingLabel := pvc.Labels != nil && pvc.Labels[sandboxLabel] == nameHash
				if !isAdoptablePool && !hasTrackingLabel {
					logger.V(4).Info("Refusing to adopt unowned PVC: missing pool authorization label or sandbox tracking label",
						"PVC.Name", pvcName, "Sandbox.Name", sandbox.Name,
						"RequiredLabel", sandboxv1beta1.SandboxAdoptableLabel, "TrackingLabel", sandboxLabel)
					return fmt.Errorf("cannot adopt unowned PVC %q: missing required pool authorization label (%q) or sandbox tracking label (%q)",
						pvcName, sandboxv1beta1.SandboxAdoptableLabel, sandboxLabel)
				}

				logger.Info("Adopting unowned PVC", "PVC.Name", pvcName, "Sandbox.Name", sandbox.Name)

				patch := client.MergeFrom(pvc.DeepCopy())
				if err := ctrl.SetControllerReference(sandbox, pvc, r.Scheme); err != nil {
					return fmt.Errorf("SetControllerReference for PVC failed: %w", err)
				}
				if err := r.Patch(ctx, pvc, patch); err != nil {
					return fmt.Errorf("patch PVC with owner reference: %w", err)
				}

			case resourceOwnedBySandbox:
			}
			continue
		}

		if !k8serrors.IsNotFound(getErr) {
			logger.Error(getErr, "Failed to get PVC")
			return fmt.Errorf("get PVC: %w", getErr)
		}

		pvcLabels := maps.Clone(pvcTemplate.Labels)
		if pvcLabels == nil {
			pvcLabels = make(map[string]string)
		}
		pvcLabels[sandboxLabel] = nameHash

		logger.Info("Creating a new PVC", "PVC.Namespace", sandbox.Namespace, "PVC.Name", pvcName)
		pvc = &corev1.PersistentVolumeClaim{
			Name:        pvcName,
			Namespace:   sandbox.Namespace,
			Annotations: maps.Clone(pvcTemplate.Annotations),
			Labels:      pvcLabels,
			Spec:        pvcTemplate.Spec,
		}
		if err := ctrl.SetControllerReference(sandbox, pvc, r.Scheme); err != nil {
			return fmt.Errorf("SetControllerReference for PVC failed: %w", err)
		}
		if err := r.Create(ctx, pvc, client.FieldOwner(sandboxControllerFieldOwner)); err != nil {
			logger.Error(err, "Failed to create PVC", "PVC.Namespace", sandbox.Namespace, "PVC.Name", pvcName)
			return err
		}
	}
	return nil
}

// handleSandboxExpiry deletes the expired sandbox's children and, per its shutdown policy, the sandbox itself.
func (r *SandboxReconciler) handleSandboxExpiry(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (bool, error) {
	var allErrors error

	podName := resolvePodName(sandbox)
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: sandbox.Namespace}, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			allErrors = errors.Join(allErrors, fmt.Errorf("get pod: %w", err))
		}
	} else {
		allErrors = errors.Join(allErrors, r.deleteExpiredChild(ctx, sandbox, pod, "pod"))
	}

	service := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: sandbox.Name, Namespace: sandbox.Namespace}, service); err != nil {
		if !k8serrors.IsNotFound(err) {
			allErrors = errors.Join(allErrors, fmt.Errorf("get service: %w", err))
		}
	} else {
		allErrors = errors.Join(allErrors, r.deleteExpiredChild(ctx, sandbox, service, "service"))
	}

	if sandbox.Spec.ShutdownPolicy != nil && *sandbox.Spec.ShutdownPolicy == sandboxv1beta1.ShutdownPolicyDelete {
		if err := r.Delete(ctx, sandbox); err != nil && !k8serrors.IsNotFound(err) {
			allErrors = errors.Join(allErrors, fmt.Errorf("delete sandbox: %w", err))
		} else {
			return true, nil
		}
	}

	if allErrors == nil {
		// Drop live-resource status while retaining terminal conditions.
		conditions := sandbox.Status.Conditions
		sandbox.Status = sandboxv1beta1.SandboxStatus{Conditions: conditions}
		setSandboxExpiredCondition(sandbox)
	}

	return false, allErrors
}

// deleteExpiredChild deletes one expired-sandbox child resource, but only when
// this sandbox controls it.
func (r *SandboxReconciler) deleteExpiredChild(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, obj client.Object, kind string) error {
	logger := log.FromContext(ctx)
	ownership, controllerRef := checkOwnership(obj, sandbox)
	switch ownership {
	case resourceOwnedBySandbox:
		if err := r.Delete(ctx, obj); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("delete %s: %w", kind, err)
		}
	case resourceUnowned:
		logger.Info("Skipping child deletion during expiry: no controllerRef pointing to this sandbox",
			"Kind", kind, "Name", obj.GetName(), "Sandbox.Name", sandbox.Name)
	case resourceOwnedByOther:
		logger.Info("Skipping child deletion during expiry: owned by a different controller",
			"Kind", kind, "Name", obj.GetName(), "Sandbox.Name", sandbox.Name,
			"Owner.Kind", controllerRef.Kind, "Owner.Name", controllerRef.Name, "Owner.UID", controllerRef.UID)
	}
	return nil
}

// CacheByObject scopes the manager cache to this controller's labeled children, so no child informer watches the whole cluster.
func CacheByObject() (map[client.Object]cache.ByObject, error) {
	sel, err := labels.Parse(sandboxLabel)
	if err != nil {
		return nil, fmt.Errorf("parse sandbox cache selector: %w", err)
	}
	return map[client.Object]cache.ByObject{
		&corev1.Pod{}:                   {Label: sel},
		&corev1.Service{}:               {Label: sel},
		&corev1.PersistentVolumeClaim{}: {Label: sel},
	}, nil
}

// MergeVolumeClaimVolumes merges PVC-backed volumes into an existing volume
// list, replacing any volumes with matching names. This follows StatefulSet
// semantics where volumeClaimTemplate volumes take priority.
func MergeVolumeClaimVolumes(existing, pvcVolumes []corev1.Volume) []corev1.Volume {
	if len(pvcVolumes) == 0 {
		return existing
	}
	vctNames := make(map[string]struct{}, len(pvcVolumes))
	for _, v := range pvcVolumes {
		vctNames[v.Name] = struct{}{}
	}
	filtered := make([]corev1.Volume, 0, len(existing))
	for _, v := range existing {
		if _, ok := vctNames[v.Name]; !ok {
			filtered = append(filtered, v)
		}
	}
	return append(filtered, pvcVolumes...)
}

// checkOwnership classifies obj's ownership relative to sandbox and returns the controller reference it read.
func checkOwnership(obj client.Object, sandbox *sandboxv1beta1.Sandbox) (resourceOwnership, *metav1.OwnerReference) {
	controllerRef := metav1.GetControllerOf(obj)
	if controllerRef == nil {
		return resourceUnowned, nil
	}
	if controllerRef.UID == sandbox.UID {
		return resourceOwnedBySandbox, controllerRef
	}
	return resourceOwnedByOther, controllerRef
}

// checkSandboxExpiry reports whether sandbox has expired and, when it has not,
// the delay to requeue after.
func checkSandboxExpiry(sandbox *sandboxv1beta1.Sandbox, now time.Time) (bool, time.Duration) {
	if sandbox.Spec.ShutdownTime == nil {
		return false, 0
	}
	shutdownTime := sandbox.Spec.ShutdownTime.Time
	if !now.Before(shutdownTime) {
		return true, 0
	}
	return false, max(shutdownTime.Sub(now), 2*time.Second)
}

func setSandboxExpiredCondition(sandbox *sandboxv1beta1.Sandbox) {
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionReady),
		Status:             metav1.ConditionFalse,
		ObservedGeneration: sandbox.Generation,
		Reason:             sandboxv1beta1.SandboxReasonExpired,
		Message:            msgSandboxExpired,
	})
}

// sandboxMarkedExpired checks if the sandbox is already marked as expired.
func sandboxMarkedExpired(sandbox *sandboxv1beta1.Sandbox) bool {
	cond := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	return cond != nil && (cond.Reason == sandboxv1beta1.SandboxReasonExpired)
}

// podReadiness describes the pod half of the Sandbox Ready condition. A pod is
// only ready once it also has a podIP, since callers cannot reach it before that.
func podReadiness(pod *corev1.Pod) (message string, ready bool) {
	if pod == nil {
		return "Pod does not exist", false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return "Pod exists with phase: " + string(pod.Status.Phase), false
	}
	idx := slices.IndexFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady
	})
	if idx < 0 || pod.Status.Conditions[idx].Status != corev1.ConditionTrue {
		return "Pod is Running but not Ready", false
	}
	if len(pod.Status.PodIPs) == 0 {
		return "Pod is Ready but has no podIPs yet", false
	}
	return "Pod is Ready", true
}

// isAdoptable reports whether obj carries the warm-pool adoptable label.
func isAdoptable(obj client.Object) bool {
	return obj.GetLabels()[sandboxv1beta1.SandboxAdoptableLabel] == "true"
}

// resolvePodName returns the name of the pod associated with the given Sandbox.
// If the sandbox has adopted a warm pool pod, the pod name is tracked in the
// agents.x-k8s.io/pod-name annotation and may differ from sandbox.Name.
func resolvePodName(sandbox *sandboxv1beta1.Sandbox) string {
	if name, ok := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; ok && name != "" {
		return name
	}
	return sandbox.Name
}

// podIPsFromStatus converts the K8s PodIP slice to a plain string slice.
func podIPsFromStatus(podIPs []corev1.PodIP) []string {
	if len(podIPs) == 0 {
		return nil
	}
	ips := make([]string, len(podIPs))
	for i, pip := range podIPs {
		ips[i] = pip.IP
	}
	return ips
}

type (
	keyPredicate func(string) bool
	keyCallback  func(string)
)

// isSystemLabel reports whether a key uses a label or annotation prefix reserved
// for the sandbox system. Such keys must never be settable through a user-supplied
// PodTemplate, otherwise a tenant could override security-critical labels (e.g. the
// headless Service selector label) and hijack another Sandbox's network traffic.
func isSystemLabel(key string) bool {
	return strings.HasPrefix(key, "agents.x-k8s.io/") ||
		strings.HasPrefix(key, "extensions.agents.x-k8s.io/")
}

// isSystemAnnotation reports whether an annotation key is reserved for the sandbox
// system and therefore must not be settable through a user-supplied PodTemplate.
func isSystemAnnotation(key string) bool {
	return isSystemLabel(key) ||
		key == asmetrics.TraceContextAnnotation
}

// isControllerManagedPodAnnotation reports whether a system-reserved annotation is one
// the core controller itself sets on a Pod during metadata reconciliation, and
// therefore must not be scrubbed during cleanup of previously-propagated annotations.
func isControllerManagedPodAnnotation(key string) bool {
	switch key {
	case sandboxv1beta1.SandboxPropagatedLabelsAnnotation,
		sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation:
		return true
	default:
		return false
	}
}

// filterSystemKeys copies src minus system-reserved keys, returning the copy and
// the sorted-by-caller list of keys it carries.
func filterSystemKeys(src map[string]string, isSystem keyPredicate, onSkip keyCallback) (map[string]string, []string) {
	out := make(map[string]string, len(src))
	var kept []string
	for k, v := range src {
		if isSystem(k) {
			onSkip(k)
			continue
		}
		out[k] = v
		kept = append(kept, k)
	}
	return out, kept
}

// extensionOwnedHashes returns the warm-pool and template-ref hashes the Pod should
// carry, which are only meaningful while an extensions controller owns the Sandbox.
func extensionOwnedHashes(sandbox *sandboxv1beta1.Sandbox) (warmPool, templateRef string) {
	ref := metav1.GetControllerOf(sandbox)
	if ref == nil {
		return "", ""
	}
	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
	if gvk.Group != extensionsv1beta1.GroupVersion.Group {
		return "", ""
	}
	if gvk.Kind == warmPoolKind {
		warmPool = sandbox.Labels[sandboxv1beta1.SandboxWarmPoolLabel]
	}
	return warmPool, sandbox.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel]
}

// setOrDelete forces m[key] to want, removing the entry when want is empty.
// It reports whether m changed.
func setOrDelete(m map[string]string, key, want string) bool {
	if want == "" {
		_, exists := m[key]
		delete(m, key)
		return exists
	}
	if m[key] == want {
		return false
	}
	m[key] = want
	return true
}

// setEntry writes m[key] = want, keeping an empty value as an empty entry rather
// than removing it. It reports whether m changed.
func setEntry(m map[string]string, key, want string) bool {
	if m[key] == want {
		return false
	}
	m[key] = want
	return true
}

// propagateKeys copies template into live, skipping system-reserved keys, and
// returns the keys it now manages plus whether live changed.
func propagateKeys(live, template map[string]string, isSystem keyPredicate, onSkip keyCallback) ([]string, bool) {
	var managed []string
	updated := false
	for k, v := range template {
		if isSystem(k) {
			onSkip(k)
			continue
		}
		if live[k] != v {
			live[k] = v
			updated = true
		}
		managed = append(managed, k)
	}
	return managed, updated
}

// prunePropagated drops previously propagated keys the template no longer carries.
// A system key recorded by an older controller is scrubbed unless keep claims it.
func prunePropagated(live map[string]string, recorded string, template map[string]string, isSystem, keep keyPredicate, onScrub keyCallback) bool {
	updated := false
	for k := range strings.SplitSeq(recorded, ",") {
		if k == "" {
			continue
		}
		if isSystem(k) {
			if keep(k) {
				continue
			}
			if _, exists := live[k]; exists {
				delete(live, k)
				updated = true
				onScrub(k)
			}
			continue
		}
		if _, ok := template[k]; !ok {
			if _, exists := live[k]; exists {
				delete(live, k)
				updated = true
			}
		}
	}
	return updated
}
