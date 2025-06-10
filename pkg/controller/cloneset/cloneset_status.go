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
	"time"

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

var (
	nowFn = func() time.Time { return time.Now() }
)

// StatusUpdater is interface for updating CloneSet status.
type StatusUpdater interface {
	UpdateCloneSetStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus, pods []*v1.Pod) error
}

func newStatusUpdater(c client.Client) StatusUpdater {
	return &realStatusUpdater{Client: c}
}

type realStatusUpdater struct {
	client.Client
}

func (r *realStatusUpdater) UpdateCloneSetStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus, pods []*v1.Pod) error {
	r.calculateStatus(cs, newStatus, pods)
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

func (r *realStatusUpdater) calculateStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus, pods []*v1.Pod) {
	coreControl := clonesetcore.New(cs)
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

	// sync progressing status after the new status is calculated.
	r.syncProgressingStatus(cs, newStatus)
}

func (r *realStatusUpdater) syncProgressingStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus) time.Duration {
	if !clonesetutils.HasProgressDeadline(cs) {
		clonesetutils.RemoveCloneSetCondition(newStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
		return time.Duration(-1)
	}

	oldStatus := cs.Status
	// revision changed, transit to CloneSetUpdated status and reset timer.
	if newStatus.UpdateRevision != oldStatus.UpdateRevision {
		klog.V(5).InfoS("CloneSet is updated", "cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus)

		msg := fmt.Sprintf("CloneSet is progressing due to revision changed from %s to %s", oldStatus.UpdateRevision, newStatus.UpdateRevision)
		condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
			v1.ConditionTrue, appsv1alpha1.CloneSetProgressUpdated, msg, nowFn)

		clonesetutils.SetCloneSetCondition(newStatus, *condition)
		return getRequeueSecondsFromCondition(condition, *cs.Spec.ProgressDeadlineSeconds, nowFn())
	}

	cond := clonesetutils.GetCloneSetCondition(oldStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
	klog.V(5).InfoS("Sync progressing status", "cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)

	isTimeoutCloneSet := cond != nil && cond.Reason == string(appsv1alpha1.CloneSetProgressDeadlineExceeded)
	// ignore scaling events.
	isAvailableCloneSet := newStatus.CurrentRevision == newStatus.UpdateRevision && cond != nil && cond.Reason == string(appsv1alpha1.CloneSetAvailable)

	if !isTimeoutCloneSet && !isAvailableCloneSet {
		if newStatus.CurrentRevision == newStatus.UpdateRevision {
			klog.V(5).InfoS("CloneSet current revision equals to update revision, waiting available ready",
				"cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
		}

		switch {
		case clonesetutils.CloneSetAvailable(cs, newStatus):
			klog.V(5).InfoS("CloneSet is available", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing, v1.ConditionTrue,
				appsv1alpha1.CloneSetAvailable, fmt.Sprintf("CloneSet is available"), nowFn)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetPartitionAvailable(cs, newStatus):
			klog.V(5).InfoS("CloneSet is partition available", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing, v1.ConditionTrue,
				appsv1alpha1.CloneSetProgressPartitionAvailable, fmt.Sprintf("CloneSet has paused due to partition ready"), nowFn)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetProgressing(cs, newStatus):
			// TODO: cloneSet rollback partition should reset timer?
			msg := "CloneSet is progressing"
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, appsv1alpha1.CloneSetProgressUpdated, msg, nowFn)

			if cond != nil {
				if cs.Status.ExpectedUpdatedReplicas > newStatus.ExpectedUpdatedReplicas {
					msg = fmt.Sprintf("CloneSet scale down due to expected updated replicas changes from %d to %d", cs.Status.ExpectedUpdatedReplicas, newStatus.ExpectedUpdatedReplicas)
				} else {
					msg = fmt.Sprintf("CloneSet scale up due to expected updated replicas changes from %d to %d", cs.Status.ExpectedUpdatedReplicas, newStatus.ExpectedUpdatedReplicas)
				}
				condition.Message = msg
			}
			klog.V(5).InfoS("CloneSet is still progressing", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "csStatus", cs.Status, "cond", cond, "msg", msg)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return getRequeueSecondsFromCondition(condition, *cs.Spec.ProgressDeadlineSeconds, nowFn())

		case clonesetutils.CloneSetDeadlineExceeded(cs, nowFn):
			msg := fmt.Sprintf("CloneSet revision %s has timed out progressing", newStatus.UpdateRevision)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionFalse, appsv1alpha1.CloneSetProgressDeadlineExceeded, msg, nowFn)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetPaused(cs):
			klog.V(5).InfoS("CloneSet is paused", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "csStatus", cs.Status, "cond", cond)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, appsv1alpha1.CloneSetProgressPaused, "CloneSet is paused", nowFn)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetResumed(cs):
			klog.V(5).InfoS("CloneSet is resumed", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, appsv1alpha1.CloneSetProgressUpdated, "CloneSet is resumed", nowFn)
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(0)
		}
	}

	klog.V(5).InfoS("CloneSet stays at previous condition", "cloneSet", klog.KObj(cs),
		"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
	newStatus.Conditions = oldStatus.Conditions

	return time.Duration(-1)
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

func getRequeueSecondsFromCondition(condition *appsv1alpha1.CloneSetCondition, pds int32, now time.Time) time.Duration {
	if condition == nil {
		return time.Duration(-1)
	}

	after := condition.LastUpdateTime.Time.Add(time.Duration(pds) * time.Second).Sub(now)
	if after < time.Second {
		return time.Duration(0)
	}
	return after + time.Second
}
