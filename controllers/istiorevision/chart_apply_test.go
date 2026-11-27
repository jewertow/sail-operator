// Copyright Istio Authors
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

package istiorevision

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "github.com/istio-ecosystem/sail-operator/api/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/scheme"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestHashChartInputs(t *testing.T) {
	base := ChartRenderInputs{
		Version:          "v1.31.0",
		Namespace:        "istio-system",
		Profile:          "default",
		Values:           map[string]any{"global": map[string]any{"istioNamespace": "istio-system", "proxy": map[string]any{"image": "proxy"}}},
		Platform:         "kubernetes",
		OCPVersion:       "",
		ChartFingerprint: "chart-v1",
		RevisionName:     "default",
	}

	t.Run("semantically unchanged values produce the same hash", func(t *testing.T) {
		reordered := base
		reordered.Values = map[string]any{"global": map[string]any{"proxy": map[string]any{"image": "proxy"}, "istioNamespace": "istio-system"}}

		baseHash, err := HashChartInputs(base)
		if err != nil {
			t.Fatalf("HashChartInputs() error = %v", err)
		}
		reorderedHash, err := HashChartInputs(reordered)
		if err != nil {
			t.Fatalf("HashChartInputs() error = %v", err)
		}
		if baseHash == "" {
			t.Fatal("Expected a non-empty input hash")
		}
		if baseHash != reorderedHash {
			t.Errorf("Expected semantically unchanged values to have the same hash, got %q and %q", baseHash, reorderedHash)
		}
	})

	changes := map[string]func(ChartRenderInputs) ChartRenderInputs{
		"version":   func(in ChartRenderInputs) ChartRenderInputs { in.Version = "v1.31.1"; return in },
		"namespace": func(in ChartRenderInputs) ChartRenderInputs { in.Namespace = "istio-system-2"; return in },
		"profile":   func(in ChartRenderInputs) ChartRenderInputs { in.Profile = "demo"; return in },
		"values": func(in ChartRenderInputs) ChartRenderInputs {
			in.Values = map[string]any{"global": map[string]any{"istioNamespace": "istio-system-2"}}
			return in
		},
		"platform":          func(in ChartRenderInputs) ChartRenderInputs { in.Platform = "openshift"; return in },
		"ocp version":       func(in ChartRenderInputs) ChartRenderInputs { in.OCPVersion = "4.17"; return in },
		"chart fingerprint": func(in ChartRenderInputs) ChartRenderInputs { in.ChartFingerprint = "chart-v2"; return in },
		"revision name":     func(in ChartRenderInputs) ChartRenderInputs { in.RevisionName = "v1-31-0"; return in },
	}
	for name, change := range changes {
		t.Run(name+" change produces a different hash", func(t *testing.T) {
			baseHash, err := HashChartInputs(base)
			if err != nil {
				t.Fatalf("HashChartInputs() error = %v", err)
			}
			changedHash, err := HashChartInputs(change(base))
			if err != nil {
				t.Fatalf("HashChartInputs() error = %v", err)
			}
			if baseHash == changedHash {
				t.Errorf("Expected changed %s to produce a different hash", name)
			}
		})
	}
}

func TestDetectChartDrift(t *testing.T) {
	newRenderedIstiodDeployment := func() *appsv1.Deployment {
		owner := metav1.OwnerReference{
			APIVersion: v1.GroupVersion.String(),
			Kind:       v1.IstioRevisionKind,
			Name:       "default",
			UID:        "test-uid",
		}
		return &appsv1.Deployment{
			Name:            "istiod",
			Namespace:       "istio-system",
			Labels:          map[string]string{"app": "istiod"},
			OwnerReferences: []metav1.OwnerReference{owner},
			Spec:            appsv1.DeploymentSpec{Replicas: new(int32)},
		}
	}

	newRenderedIstiodService := func() *corev1.Service {
		return &corev1.Service{
			Name: "istiod", Namespace: "istio-system", Annotations: map[string]string{"owner": "sail"},
		}
	}

	newRenderedChartResources := func() []client.Object {
		return []client.Object{newRenderedIstiodDeployment(), newRenderedIstiodService()}
	}

	t.Run("unchanged resources have no drift", func(t *testing.T) {
		drift, err := DetectChartDrift(context.Background(), newFakeClientWithObjects(newRenderedChartResources()...), newRenderedChartResources())
		if err != nil {
			t.Fatalf("DetectChartDrift() error = %v", err)
		}
		if drift.Detected {
			t.Error("Expected no drift for unchanged resources")
		}
	})

	t.Run("missing chart-owned resource is drift", func(t *testing.T) {
		cl := newFakeClientWithObjects(newRenderedIstiodDeployment())
		drift, err := DetectChartDrift(context.Background(), cl, newRenderedChartResources())
		if err != nil {
			t.Fatalf("DetectChartDrift() error = %v", err)
		}
		if !drift.Detected {
			t.Error("Expected drift when an expected resource is missing")
		}
	})

	t.Run("edited chart-owned resource is drift", func(t *testing.T) {
		tests := []struct {
			name             string
			mutateDeployment func(*appsv1.Deployment)
			mutateService    func(*corev1.Service)
		}{
			{
				name: "edited spec",
				mutateDeployment: func(deployment *appsv1.Deployment) {
					replicas := int32(2)
					deployment.Spec.Replicas = &replicas
				},
			},
			{
				name: "edited labels",
				mutateDeployment: func(deployment *appsv1.Deployment) {
					deployment.Labels["drift"] = "true"
				},
			},
			{
				name: "edited annotations",
				mutateService: func(service *corev1.Service) {
					service.Annotations["drift"] = "true"
				},
			},
			{
				name: "edited owner references",
				mutateService: func(service *corev1.Service) {
					service.OwnerReferences = []metav1.OwnerReference{{Name: "other-owner"}}
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				expected := newRenderedChartResources()
				deployment := newRenderedIstiodDeployment()
				service := newRenderedIstiodService()
				if test.mutateDeployment != nil {
					test.mutateDeployment(deployment)
				}
				if test.mutateService != nil {
					test.mutateService(service)
				}

				drift, err := DetectChartDrift(context.Background(), newFakeClientWithObjects(deployment, service), expected)
				if err != nil {
					t.Fatalf("DetectChartDrift() error = %v", err)
				}
				if !drift.Detected {
					t.Error("Expected drift when a chart-owned resource is edited")
				}
			})
		}
	})

	t.Run("status-only mutation is not drift", func(t *testing.T) {
		expected := newRenderedChartResources()
		deployment := newRenderedIstiodDeployment()
		deployment.Status.ReadyReplicas = 3
		service := newRenderedIstiodService()
		service.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.1"}}
		drift, err := DetectChartDrift(context.Background(), newFakeClientWithObjects(deployment, service), expected)
		if err != nil {
			t.Fatalf("DetectChartDrift() error = %v", err)
		}
		if drift.Detected {
			t.Error("Expected no drift for status-only changes")
		}
	})

	t.Run("resource comparison failure is returned", func(t *testing.T) {
		expected := newRenderedChartResources()
		compareErr := errors.New("get failed")
		cl := fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithObjects(expected...).
			WithInterceptorFuncs(interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return compareErr
			}}).
			Build()
		drift, err := DetectChartDrift(context.Background(), cl, expected)
		if !errors.Is(err, compareErr) {
			t.Fatalf("DetectChartDrift() error = %v, want %v", err, compareErr)
		}
		if !drift.Detected {
			t.Error("Expected comparison failure to report drift so the controller fails closed")
		}
	})
}

func TestShouldApplyChart(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	inputs := ChartRenderInputs{Version: "v1.31.0", Namespace: "istio-system", RevisionName: "default", ChartFingerprint: "chart-v1"}
	inputHash, err := HashChartInputs(inputs)
	if err != nil {
		t.Fatalf("HashChartInputs() error = %v", err)
	}
	applied := ChartApplyState{
		LastAppliedInputHash:    inputHash,
		LastSuccessfulApplyTime: now.Add(-10 * time.Minute),
		ReleaseExists:           true,
	}

	tests := []struct {
		name       string
		inputs     ChartRenderInputs
		state      ChartApplyState
		drift      ChartDrift
		wantApply  bool
		wantReason ChartApplyReason
	}{
		{
			name:       "initial revision applies",
			state:      ChartApplyState{},
			wantApply:  true,
			wantReason: ChartApplyReasonInitial,
		},
		{
			name:       "pod churn without input change or drift does not apply",
			inputs:     inputs,
			state:      applied,
			wantApply:  false,
			wantReason: ChartApplyReasonNone,
		},
		{
			name:       "status self-trigger without input change or drift does not apply",
			inputs:     inputs,
			state:      applied,
			wantApply:  false,
			wantReason: ChartApplyReasonNone,
		},
		{
			name:       "changed version applies",
			inputs:     withVersion(inputs, "v1.31.1"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed namespace applies",
			inputs:     withNamespace(inputs, "istio-system-2"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed profile applies",
			inputs:     withProfile(inputs, "demo"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed values apply",
			inputs:     withValues(inputs, map[string]any{"global": map[string]any{"istioNamespace": "istio-system-2"}}),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed platform applies",
			inputs:     withPlatform(inputs, "openshift"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed OCP version applies",
			inputs:     withOCPVersion(inputs, "4.17"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "changed operator chart applies",
			inputs:     withChartFingerprint(inputs, "chart-v2"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "missing Helm release applies",
			state:      withReleaseExists(applied, false),
			wantApply:  true,
			wantReason: ChartApplyReasonReleaseMissing,
		},
		{
			name:       "pending Helm release applies",
			state:      withReleasePending(applied, true),
			wantApply:  true,
			wantReason: ChartApplyReasonReleasePending,
		},
		{
			name:       "failed Helm release applies",
			state:      withReleaseFailed(applied, true),
			wantApply:  true,
			wantReason: ChartApplyReasonReleaseFailed,
		},
		{
			name:       "deleted chart-owned resource applies immediately",
			state:      applied,
			drift:      ChartDrift{Detected: true},
			wantApply:  true,
			wantReason: ChartApplyReasonDriftDetected,
		},
		{
			name:       "edited chart-owned resource applies immediately",
			state:      applied,
			drift:      ChartDrift{Detected: true},
			wantApply:  true,
			wantReason: ChartApplyReasonDriftDetected,
		},
		{
			name:       "comparison error fails closed",
			state:      applied,
			drift:      ChartDrift{Err: errors.New("comparison failed")},
			wantApply:  true,
			wantReason: ChartApplyReasonComparisonError,
		},
		{
			name:       "backstop due applies",
			state:      withLastSuccessfulApply(applied, now.Add(DefaultChartBackstopInterval-time.Minute)),
			wantApply:  true,
			wantReason: ChartApplyReasonBackstopDue,
		},
		{
			name:       "in-place upgrade applies through changed input hash",
			inputs:     withVersion(inputs, "v1.31.1"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
		{
			name:       "revision-based upgrade applies through a new revision",
			inputs:     withRevisionName(inputs, "v1-31-1"),
			state:      applied,
			wantApply:  true,
			wantReason: ChartApplyReasonInputChanged,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := ShouldApplyChart(now, tt.inputs, tt.state, tt.drift)
			if err != nil {
				t.Fatalf("ShouldApplyChart() error = %v", err)
			}
			if decision.Apply != tt.wantApply {
				t.Errorf("ShouldApplyChart().Apply = %v, want %v", decision.Apply, tt.wantApply)
			}
			if decision.Reason != tt.wantReason {
				t.Errorf("ShouldApplyChart().Reason = %q, want %q", decision.Reason, tt.wantReason)
			}
		})
	}
}

func TestShouldApplyChartBackstopScheduling(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	inputs := ChartRenderInputs{Version: "v1.31.0", RevisionName: "default"}
	inputHash, err := HashChartInputs(inputs)
	if err != nil {
		t.Fatalf("HashChartInputs() error = %v", err)
	}
	state := ChartApplyState{LastAppliedInputHash: inputHash, LastSuccessfulApplyTime: now.Add(-30 * time.Minute), ReleaseExists: true}

	decision, err := ShouldApplyChart(now, inputs, state, ChartDrift{})
	if err != nil {
		t.Fatalf("ShouldApplyChart() error = %v", err)
	}
	wantRequeue := 30 * time.Minute
	if decision.RequeueAfter != wantRequeue {
		t.Errorf("ShouldApplyChart().RequeueAfter = %v, want %v", decision.RequeueAfter, wantRequeue)
	}

	state = RecordSuccessfulChartApply(state, now, inputHash)
	if !state.LastSuccessfulApplyTime.Equal(now) {
		t.Errorf("RecordSuccessfulChartApply().LastSuccessfulApplyTime = %v, want %v", state.LastSuccessfulApplyTime, now)
	}
	if state.LastAppliedInputHash != inputHash {
		t.Errorf("RecordSuccessfulChartApply().LastAppliedInputHash = %q, want %q", state.LastAppliedInputHash, inputHash)
	}
}

func TestRecordSuccessfulChartApplyDoesNotChangeOnNoOp(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	before := ChartApplyState{LastAppliedInputHash: "hash", LastSuccessfulApplyTime: now.Add(-time.Minute), ReleaseExists: true}
	after := RecordSuccessfulChartApply(before, now.Add(time.Minute), "hash")
	if !reflect.DeepEqual(before, after) {
		t.Errorf("RecordSuccessfulChartApply() changed state for an unchanged successful input: before %+v, after %+v", before, after)
	}
}

func TestRecordFailedChartApplyDoesNotAdvanceState(t *testing.T) {
	before := ChartApplyState{
		LastAppliedInputHash:    "old-hash",
		LastSuccessfulApplyTime: time.Date(2026, 11, 27, 11, 0, 0, 0, time.UTC),
		ReleaseExists:           true,
	}
	after := RecordFailedChartApply(before)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("RecordFailedChartApply() changed state: before %+v, after %+v", before, after)
	}
}

func TestRepeatedNoOpReconciliationsDoNotPostponeBackstop(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	inputs := ChartRenderInputs{Version: "v1.31.0", RevisionName: "default"}
	inputHash, err := HashChartInputs(inputs)
	if err != nil {
		t.Fatalf("HashChartInputs() error = %v", err)
	}
	state := ChartApplyState{
		LastAppliedInputHash:    inputHash,
		LastSuccessfulApplyTime: now.Add(-30 * time.Minute),
		ReleaseExists:           true,
	}

	first, err := ShouldApplyChart(now, inputs, state, ChartDrift{})
	if err != nil {
		t.Fatalf("first ShouldApplyChart() error = %v", err)
	}
	second, err := ShouldApplyChart(now.Add(time.Minute), inputs, state, ChartDrift{})
	if err != nil {
		t.Fatalf("second ShouldApplyChart() error = %v", err)
	}
	if first.RequeueAfter != 30*time.Minute {
		t.Errorf("first RequeueAfter = %v, want 30m", first.RequeueAfter)
	}
	if second.RequeueAfter != 29*time.Minute {
		t.Errorf("second RequeueAfter = %v, want 29m", second.RequeueAfter)
	}
	if !state.LastSuccessfulApplyTime.Equal(now.Add(-30 * time.Minute)) {
		t.Errorf("no-op reconciliations changed last successful apply time: %v", state.LastSuccessfulApplyTime)
	}
}

func newFakeClientWithObjects(objects ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()
}

func withVersion(in ChartRenderInputs, value string) ChartRenderInputs {
	in.Version = value
	return in
}

func withNamespace(in ChartRenderInputs, value string) ChartRenderInputs {
	in.Namespace = value
	return in
}

func withProfile(in ChartRenderInputs, value string) ChartRenderInputs {
	in.Profile = value
	return in
}

func withValues(in ChartRenderInputs, value map[string]any) ChartRenderInputs {
	in.Values = value
	return in
}

func withPlatform(in ChartRenderInputs, value string) ChartRenderInputs {
	in.Platform = value
	return in
}

func withOCPVersion(in ChartRenderInputs, value string) ChartRenderInputs {
	in.OCPVersion = value
	return in
}

func withChartFingerprint(in ChartRenderInputs, value string) ChartRenderInputs {
	in.ChartFingerprint = value
	return in
}

func withRevisionName(in ChartRenderInputs, value string) ChartRenderInputs {
	in.RevisionName = value
	return in
}

func withReleaseExists(in ChartApplyState, value bool) ChartApplyState {
	in.ReleaseExists = value
	return in
}

func withReleasePending(in ChartApplyState, value bool) ChartApplyState {
	in.ReleasePending = value
	return in
}

func withReleaseFailed(in ChartApplyState, value bool) ChartApplyState {
	in.ReleaseFailed = value
	return in
}

func withLastSuccessfulApply(in ChartApplyState, value time.Time) ChartApplyState {
	in.LastSuccessfulApplyTime = value
	return in
}
