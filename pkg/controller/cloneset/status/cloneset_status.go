/*
Copyright 2019 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloneset

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	clonesetcore "github.com/openkruise/kruise/pkg/controller/cloneset/core"
	"github.com/openkruise/kruise/pkg/controller/cloneset/sync"
	clonesetutils "github.com/openkruise/kruise/pkg/controller/cloneset/utils"
	"github.com/openkruise/kruise/pkg/util"
)

// Updater is interface for updating CloneSet status.
type Updater interface {
	UpdateCloneSetStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus, pods []*v1.Pod) error
	CalculateStatus(cs *appsv1alpha1.CloneSet, currentRevision string, updateRevision string, selector string, collisionCount int32, pods []*v1.Pod) *appsv1alpha1.CloneSetStatus
}

func NewStatusUpdater(c client.Client) Updater {
	return &realStatusUpdater{Client: c}
}

type realStatusUpdater struct {
	client.Client
}

func (r *realStatusUpdater) UpdateCloneSetStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus, pods []*v1.Pod) error {
	if err := clonesetcore.New(cs).ExtraStatusCalculation(newStatus, pods); err != nil {
		return fmt.Errorf("failed to calculate extra status for cloneSet %s/%s: %v", cs.Namespace, cs.Name, err)
	}
	if !r.inconsistentStatus(cs, newStatus) {
		return nil
	}
	klog.InfoS("To update CloneSet status", "cloneSet", klog.KObj(cs), "replicas", newStatus.Replicas, "ready", newStatus.ReadyReplicas, "available", newStatus.AvailableReplicas,
		"updated", newStatus.UpdatedReplicas, "updatedReady", newStatus.UpdatedReadyReplicas, "currentRevision", newStatus.CurrentRevision, "updateRevision", newStatus.UpdateRevision)
	return r.updateStatus(cs, newStatus)
}

func (r *realStatusUpdater) updateStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		clone := &appsv1alpha1.CloneSet{}
		if err := r.Get(context.TODO(), types.NamespacedName{Namespace: cs.Namespace, Name: cs.Name}, clone); err != nil {
			return err
		}
		clone.Status = *newStatus
		return r.Status().Update(context.TODO(), clone)
	})
}

func (r *realStatusUpdater) inconsistentStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus) bool {
	oldStatus := cs.Status
	return newStatus.ObservedGeneration > oldStatus.ObservedGeneration ||
		newStatus.Replicas != oldStatus.Replicas ||
		newStatus.ReadyReplicas != oldStatus.ReadyReplicas ||
		newStatus.AvailableReplicas != oldStatus.AvailableReplicas ||
		newStatus.UpdatedReadyReplicas != oldStatus.UpdatedReadyReplicas ||
		newStatus.UpdatedReplicas != oldStatus.UpdatedReplicas ||
		newStatus.UpdatedAvailableReplicas != oldStatus.UpdatedAvailableReplicas ||
		newStatus.ExpectedUpdatedReplicas != oldStatus.ExpectedUpdatedReplicas ||
		newStatus.UpdateRevision != oldStatus.UpdateRevision ||
		newStatus.CurrentRevision != oldStatus.CurrentRevision ||
		newStatus.LabelSelector != oldStatus.LabelSelector ||
		hasProgressingConditionChanged(cs.Status, *newStatus)
}

func (r *realStatusUpdater) CalculateStatus(cs *appsv1alpha1.CloneSet, currentRevision string,
	updateRevision string, selector string, collisionCount int32, pods []*v1.Pod) *appsv1alpha1.CloneSetStatus {
	coreControl := clonesetcore.New(cs)
	newStatus := appsv1alpha1.CloneSetStatus{
		ObservedGeneration: cs.Generation,
		CurrentRevision:    currentRevision,
		UpdateRevision:     updateRevision,
		CollisionCount:     new(int32),
		LabelSelector:      selector,
	}
	*newStatus.CollisionCount = collisionCount

	for _, pod := range pods {
		newStatus.Replicas++
		if coreControl.IsPodUpdateReady(pod, 0) {
			newStatus.ReadyReplicas++
		}
		if sync.IsPodAvailable(coreControl, pod, cs.Spec.MinReadySeconds) {
			newStatus.AvailableReplicas++
		}
		if clonesetutils.EqualToRevisionHash("", pod, newStatus.UpdateRevision) {
			newStatus.UpdatedReplicas++
		}
		if clonesetutils.EqualToRevisionHash("", pod, newStatus.UpdateRevision) && coreControl.IsPodUpdateReady(pod, 0) {
			newStatus.UpdatedReadyReplicas++
		}
		if clonesetutils.EqualToRevisionHash("", pod, newStatus.UpdateRevision) && sync.IsPodAvailable(coreControl, pod, cs.Spec.MinReadySeconds) {
			newStatus.UpdatedAvailableReplicas++
		}
	}
	// Consider the update revision as stable if revisions of all pods are consistent to it and have the expected number of replicas, no need to wait all of them ready
	if newStatus.UpdatedReplicas == newStatus.Replicas && newStatus.Replicas == *cs.Spec.Replicas {
		newStatus.CurrentRevision = newStatus.UpdateRevision
	}

	if partition, err := util.CalculatePartitionReplicas(cs.Spec.UpdateStrategy.Partition, cs.Spec.Replicas); err == nil {
		newStatus.ExpectedUpdatedReplicas = *cs.Spec.Replicas - int32(partition)
	}

	return &newStatus
}

func hasProgressingConditionChanged(oldStatus appsv1alpha1.CloneSetStatus, newStatus appsv1alpha1.CloneSetStatus) bool {
	oldCond := clonesetutils.GetCloneSetCondition(oldStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
	newCond := clonesetutils.GetCloneSetCondition(newStatus, appsv1alpha1.CloneSetConditionTypeProgressing)

	if oldCond == nil && newCond == nil {
		return false
	}

	if (oldCond == nil) != (newCond == nil) {
		return true
	}

	return oldCond.Status != newCond.Status ||
		oldCond.Reason != newCond.Reason ||
		oldCond.Message != newCond.Message ||
		!oldCond.LastTransitionTime.Equal(&newCond.LastTransitionTime)
}
