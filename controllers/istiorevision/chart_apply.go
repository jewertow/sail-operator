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
	"time"

	v1 "github.com/istio-ecosystem/sail-operator/api/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultChartBackstopInterval = time.Hour

	ChartApplyInputHashAnnotation           = "sailoperator.io/chart-input-hash"
	ChartApplyLastSuccessfulApplyAnnotation = "sailoperator.io/last-successful-chart-apply"
)

type ChartRenderInputs struct {
	Version          string
	Namespace        string
	Profile          string
	Values           map[string]any
	Platform         string
	OCPVersion       string
	ChartFingerprint string
	RevisionName     string
}

type ChartApplyState struct {
	LastAppliedInputHash    string
	LastSuccessfulApplyTime time.Time
	ReleaseExists           bool
	ReleasePending          bool
	ReleaseFailed           bool
}

type ChartDrift struct {
	Detected bool
	Err      error
}

type ChartApplyReason string

const (
	ChartApplyReasonNone            ChartApplyReason = ""
	ChartApplyReasonInitial         ChartApplyReason = "Initial"
	ChartApplyReasonInputChanged    ChartApplyReason = "InputChanged"
	ChartApplyReasonDriftDetected   ChartApplyReason = "DriftDetected"
	ChartApplyReasonBackstopDue     ChartApplyReason = "BackstopDue"
	ChartApplyReasonReleaseMissing  ChartApplyReason = "ReleaseMissing"
	ChartApplyReasonReleasePending  ChartApplyReason = "ReleasePending"
	ChartApplyReasonReleaseFailed   ChartApplyReason = "ReleaseFailed"
	ChartApplyReasonComparisonError ChartApplyReason = "ComparisonError"
)

type ChartApplyDecision struct {
	Apply        bool
	Reason       ChartApplyReason
	RequeueAfter time.Duration
}

func HashChartInputs(inputs ChartRenderInputs) (string, error) {
	return "", nil
}

func ChartRenderInputsForRevision(cfg config.ReconcilerConfig, rev *v1.IstioRevision) ChartRenderInputs {
	return ChartRenderInputs{}
}

func DetectChartDrift(ctx context.Context, cl client.Client, expected []client.Object) (ChartDrift, error) {
	return ChartDrift{}, nil
}

func ShouldApplyChart(
	now time.Time,
	inputs ChartRenderInputs,
	state ChartApplyState,
	drift ChartDrift,
) (ChartApplyDecision, error) {
	return ChartApplyDecision{}, nil
}

func RecordSuccessfulChartApply(state ChartApplyState, now time.Time, inputHash string) ChartApplyState {
	return state
}

func RecordFailedChartApply(state ChartApplyState) ChartApplyState {
	return state
}
