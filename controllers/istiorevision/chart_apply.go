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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	v1 "github.com/istio-ecosystem/sail-operator/api/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/config"
	"github.com/istio-ecosystem/sail-operator/pkg/constants"
	"github.com/istio-ecosystem/sail-operator/pkg/helm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultChartBackstopInterval = time.Hour

	ChartApplyInputHashAnnotation           = "sailoperator.io/chart-input-hash"
	ChartApplyLastSuccessfulApplyAnnotation = "sailoperator.io/last-successful-chart-apply"
)

type ChartRenderInputs struct {
	Version           string
	Namespace         string
	OperatorNamespace string
	Profile           string
	Values            map[string]any
	Platform          string
	OCPVersion        string
	ChartFingerprint  string
	RevisionName      string
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
	encoded, err := json.Marshal(inputs)
	if err != nil {
		return "", fmt.Errorf("failed to encode chart inputs: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func ChartRenderInputsForRevision(cfg config.ReconcilerConfig, rev *v1.IstioRevision) ChartRenderInputs {
	values := helm.FromValues(rev.Spec.Values)
	ocpVersion := ""
	if cfg.OCPVersion != nil {
		ocpVersion = fmt.Sprintf("%d.%d", cfg.OCPVersion.Major, cfg.OCPVersion.Minor)
	}

	chartPaths := []string{chartPath(rev.Spec.Version, constants.IstiodChartName)}
	if rev.Name == v1.DefaultRevision {
		chartPaths = append(chartPaths, chartPath(rev.Spec.Version, constants.BaseChartName))
	}

	return ChartRenderInputs{
		Version:           rev.Spec.Version,
		Namespace:         rev.Spec.Namespace,
		OperatorNamespace: cfg.OperatorNamespace,
		Profile:           cfg.DefaultProfile,
		Values:            values,
		Platform:          string(cfg.Platform),
		OCPVersion:        ocpVersion,
		ChartFingerprint:  fingerprintChartPaths(cfg.ResourceFS, chartPaths),
		RevisionName:      rev.Name,
	}
}

func DetectChartDrift(ctx context.Context, cl client.Client, expected []client.Object) (ChartDrift, error) {
	for _, wanted := range expected {
		live, ok := wanted.DeepCopyObject().(client.Object)
		if !ok {
			err := fmt.Errorf("failed to copy expected %s %s/%s", wanted.GetObjectKind().GroupVersionKind().Kind, wanted.GetNamespace(), wanted.GetName())
			return ChartDrift{Detected: true, Err: err}, err
		}

		key := client.ObjectKeyFromObject(live)
		if err := cl.Get(ctx, key, live); err != nil {
			if apierrors.IsNotFound(err) {
				return ChartDrift{Detected: true}, nil
			}
			err = fmt.Errorf("failed to get %s %s: %w", wanted.GetObjectKind().GroupVersionKind().Kind, key, err)
			return ChartDrift{Detected: true, Err: err}, err
		}

		if !semanticResourceEqual(wanted, live) {
			return ChartDrift{Detected: true}, nil
		}
	}
	return ChartDrift{}, nil
}

func ShouldApplyChart(
	now time.Time,
	inputs ChartRenderInputs,
	state ChartApplyState,
	drift ChartDrift,
) (ChartApplyDecision, error) {
	inputHash, err := HashChartInputs(inputs)
	if err != nil {
		return ChartApplyDecision{}, err
	}

	apply := func(reason ChartApplyReason) ChartApplyDecision {
		return ChartApplyDecision{Apply: true, Reason: reason}
	}

	if state.LastAppliedInputHash == "" && state.LastSuccessfulApplyTime.IsZero() && !state.ReleaseExists {
		return apply(ChartApplyReasonInitial), nil
	}
	if !state.ReleaseExists {
		return apply(ChartApplyReasonReleaseMissing), nil
	}
	if state.ReleasePending {
		return apply(ChartApplyReasonReleasePending), nil
	}
	if state.ReleaseFailed {
		return apply(ChartApplyReasonReleaseFailed), nil
	}
	if state.LastAppliedInputHash != inputHash {
		return apply(ChartApplyReasonInputChanged), nil
	}
	if drift.Err != nil {
		return apply(ChartApplyReasonComparisonError), nil
	}
	if drift.Detected {
		return apply(ChartApplyReasonDriftDetected), nil
	}
	if !state.LastSuccessfulApplyTime.IsZero() && !now.Before(state.LastSuccessfulApplyTime.Add(DefaultChartBackstopInterval)) {
		return apply(ChartApplyReasonBackstopDue), nil
	}

	return ChartApplyDecision{RequeueAfter: state.LastSuccessfulApplyTime.Add(DefaultChartBackstopInterval).Sub(now)}, nil
}

func RecordSuccessfulChartApply(state ChartApplyState, now time.Time, inputHash string) ChartApplyState {
	state.LastAppliedInputHash = inputHash
	state.LastSuccessfulApplyTime = now
	state.ReleaseExists = true
	state.ReleasePending = false
	state.ReleaseFailed = false
	return state
}

func RecordFailedChartApply(state ChartApplyState) ChartApplyState {
	return state
}

func chartPath(version, chartName string) string {
	return path.Join(version, "charts", chartName)
}

func fingerprintChartPaths(resourceFS fs.FS, paths []string) string {
	hasher := sha256.New()
	for _, chartPath := range paths {
		_, _ = hasher.Write([]byte(chartPath + "\n"))
		_ = fs.WalkDir(resourceFS, chartPath, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			_, _ = hasher.Write([]byte(path + "\n"))
			contents, readErr := fs.ReadFile(resourceFS, path)
			if readErr != nil {
				return readErr
			}
			_, _ = hasher.Write(contents)
			_, _ = hasher.Write([]byte("\n"))
			return nil
		})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func semanticResourceEqual(wanted, live client.Object) bool {
	wantedCopy, ok := wanted.DeepCopyObject().(client.Object)
	if !ok {
		return false
	}
	liveCopy, ok := live.DeepCopyObject().(client.Object)
	if !ok {
		return false
	}

	normalizeResourceForComparison(wantedCopy)
	normalizeResourceForComparison(liveCopy)
	return reflect.DeepEqual(wantedCopy, liveCopy)
}

func normalizeResourceForComparison(obj client.Object) {
	obj.SetResourceVersion("")
	obj.SetGeneration(0)
	obj.SetUID("")
	obj.SetManagedFields(nil)
	obj.SetCreationTimestamp(metav1.Time{})
	obj.SetSelfLink("")
	obj.SetGenerateName("")
	obj.SetDeletionTimestamp(nil)
	obj.SetDeletionGracePeriodSeconds(nil)
	obj.SetFinalizers(nil)

	value := reflect.ValueOf(obj).Elem()
	typeMeta := value.FieldByName("TypeMeta")
	if typeMeta.IsValid() && typeMeta.CanSet() {
		typeMeta.Set(reflect.Zero(typeMeta.Type()))
	}
	status := value.FieldByName("Status")
	if status.IsValid() && status.CanSet() {
		status.Set(reflect.Zero(status.Type()))
	}
}

func chartResourcesFromManifest(scheme *runtime.Scheme, manifest string) ([]client.Object, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	objects := make([]client.Object, 0)
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode chart manifest: %w", err)
		}
		if len(document) == 0 {
			continue
		}

		obj, err := objectFromDocument(scheme, document)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].GetName() < objects[j].GetName()
	})
	return objects, nil
}

func objectFromDocument(scheme *runtime.Scheme, document map[string]any) (client.Object, error) {
	raw := unstructured.Unstructured{Object: document}
	groupVersionKind := raw.GroupVersionKind()
	if groupVersionKind.Empty() {
		return nil, fmt.Errorf("chart manifest document has no API version or kind")
	}

	object, err := scheme.New(groupVersionKind)
	if err != nil {
		return nil, fmt.Errorf("failed to create object for %s: %w", groupVersionKind, err)
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(document, object); err != nil {
		return nil, fmt.Errorf("failed to convert %s: %w", groupVersionKind, err)
	}

	clientObject, ok := object.(client.Object)
	if !ok {
		return nil, fmt.Errorf("object for %s is not a Kubernetes client object", groupVersionKind)
	}
	return clientObject, nil
}
