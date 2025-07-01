package sync

import (
	"fmt"
	"time"

	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	clonesetutils "github.com/openkruise/kruise/pkg/controller/cloneset/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var (
	timer clock.Clock = clock.RealClock{}
)

func (r *realControl) syncProgressingStatus(cs *appsv1alpha1.CloneSet, newStatus *appsv1alpha1.CloneSetStatus) time.Duration {
	oldStatus := cs.Status
	cond := clonesetutils.GetCloneSetCondition(oldStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
	if !clonesetutils.HasProgressDeadline(cs) {
		if cond != nil {
			clonesetutils.RemoveCloneSetCondition(newStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
		}
		return time.Duration(-1)
	}

	// revision changed, transit to CloneSetUpdated status.
	if newStatus.UpdateRevision != oldStatus.UpdateRevision {
		klog.V(5).InfoS("CloneSet is updated", "cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus)

		msg := fmt.Sprintf("CloneSet is progressing due to revision changed from %s to %s", oldStatus.UpdateRevision, newStatus.UpdateRevision)
		condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
			v1.ConditionTrue, appsv1alpha1.CloneSetProgressUpdated, msg, timer.Now())

		clonesetutils.SetCloneSetCondition(newStatus, *condition)
		return getRequeueSecondsFromCondition(condition, *cs.Spec.ProgressDeadlineSeconds, timer.Now())
	}

	klog.V(5).InfoS("Sync progressing status", "cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
	isTimeoutCloneSet := cond != nil && cond.Reason == string(appsv1alpha1.CloneSetProgressDeadlineExceeded)
	isAvailableCloneSet := newStatus.CurrentRevision == newStatus.UpdateRevision && cond != nil && cond.Reason == string(appsv1alpha1.CloneSetAvailable)

	if !isTimeoutCloneSet && !isAvailableCloneSet {
		if newStatus.CurrentRevision == newStatus.UpdateRevision {
			klog.V(5).InfoS("CloneSet current revision equals to update revision, waiting available ready",
				"cloneSet", klog.KObj(cs), "newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
		}

		switch {
		case clonesetutils.CloneSetPaused(cs):
			klog.V(5).InfoS("CloneSet is paused", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "csStatus", cs.Status, "cond", cond)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, appsv1alpha1.CloneSetProgressPaused, "CloneSet is paused", timer.Now())
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetPartitionAvailable(cs, newStatus):
			klog.V(5).InfoS("CloneSet is partition available", "cloneSet", klog.KObj(cs),
				"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)

			msg := "CloneSet has paused due to partition ready"
			reason := appsv1alpha1.CloneSetProgressPartitionAvailable

			if clonesetutils.CloneSetAvailable(cs, newStatus) {
				reason, msg = appsv1alpha1.CloneSetAvailable, "CloneSet is available"
			}

			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, reason, msg, timer.Now())
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetProgressing(cs, newStatus):
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionTrue, appsv1alpha1.CloneSetProgressUpdated, "CloneSet is progressing", timer.Now())

			if cond != nil {
				if cond.Reason == string(appsv1alpha1.CloneSetProgressPaused) {
					condition.Message = fmt.Sprintf("CloneSet is resumed")
				}
				clonesetutils.RemoveCloneSetCondition(newStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
			}
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)

		case clonesetutils.CloneSetDeadlineExceeded(cs, timer.Now()):
			msg := fmt.Sprintf("CloneSet revision %s has timed out progressing", newStatus.UpdateRevision)
			condition := clonesetutils.NewCloneSetCondition(appsv1alpha1.CloneSetConditionTypeProgressing,
				v1.ConditionFalse, appsv1alpha1.CloneSetProgressDeadlineExceeded, msg, timer.Now())
			clonesetutils.SetCloneSetCondition(newStatus, *condition)
			return time.Duration(-1)
		}
	}

	klog.V(5).InfoS("CloneSet stays at previous condition", "cloneSet", klog.KObj(cs),
		"newStatus", newStatus, "oldStatus", oldStatus, "cond", cond)
	newStatus.Conditions = oldStatus.Conditions

	return time.Duration(-1)
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
