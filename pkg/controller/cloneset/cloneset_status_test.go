package cloneset

import (
	"testing"
	"time"

	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	clonesetutils "github.com/openkruise/kruise/pkg/controller/cloneset/utils"
	"github.com/openkruise/kruise/pkg/util"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func TestSyncProgressingStatus(t *testing.T) {
	progressDeadlineSeconds := ptr.To(int32(10))

	timeFn := func(min, sec int) time.Time {
		return time.Date(2016, 1, 1, 0, min, sec, 0, time.UTC)
	}

	newStatus := func(replicas, readyReplicas, availableReplicas, updatedReplicas, updatedReadyReplicas,
		updatedAvailableReplicas, expectedUpdatedReplicas int32, currentRevision, updateRevision string) *appsv1alpha1.CloneSetStatus {
		return &appsv1alpha1.CloneSetStatus{
			Replicas:                 replicas,
			ReadyReplicas:            readyReplicas,
			AvailableReplicas:        availableReplicas,
			UpdatedReplicas:          updatedReplicas,
			UpdatedReadyReplicas:     updatedReadyReplicas,
			UpdatedAvailableReplicas: updatedAvailableReplicas,
			ExpectedUpdatedReplicas:  expectedUpdatedReplicas,
			CurrentRevision:          currentRevision,
			UpdateRevision:           updateRevision,
		}
	}

	tests := []struct {
		name          string
		cs            *appsv1alpha1.CloneSet
		nowFn         func() time.Time
		newStatus     *appsv1alpha1.CloneSetStatus
		wantCond      *appsv1alpha1.CloneSetCondition
		expectEnqueue time.Duration
	}{
		{
			name: "remove ProgressDeadlineSeconds",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:      []appsv1alpha1.CloneSetCondition{{Type: appsv1alpha1.CloneSetConditionTypeProgressing, Reason: string(appsv1alpha1.CloneSetProgressUpdated)}},
					CurrentRevision: "1",
					UpdateRevision:  "2",
				},
			},
			nowFn:         func() time.Time { return timeFn(0, 0) },
			newStatus:     newStatus(0, 0, 0, 0, 0, 0, 0, "1", "3"),
			wantCond:      nil,
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with nil condition, and start a new revision",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{ProgressDeadlineSeconds: progressDeadlineSeconds},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:      nil,
					CurrentRevision: "1",
					UpdateRevision:  "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(0, 0, 0, 0, 0, 0, 0, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet is progressing due to revision changed from 1 to 2",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: 11 * time.Second,
		},
		{
			name: "startup with nil condition, and CloneSet is available",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{Replicas: ptr.To(int32(1)), ProgressDeadlineSeconds: progressDeadlineSeconds},
				Status: appsv1alpha1.CloneSetStatus{
					Replicas:                 1,
					ReadyReplicas:            1,
					AvailableReplicas:        0,
					UpdatedReplicas:          1,
					UpdatedReadyReplicas:     1,
					UpdatedAvailableReplicas: 0,
					ExpectedUpdatedReplicas:  1,
					UpdateRevision:           "2",
					CurrentRevision:          "2",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(1, 1, 1, 1, 1, 1, 1, "2", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetAvailable),
				Message:            "CloneSet is available",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with nil condition, CurrentRevision equals to UpdateRevision but CloneSet is not available and paused",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{Replicas: ptr.To(int32(1)), UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Paused: true}, ProgressDeadlineSeconds: progressDeadlineSeconds},
				Status: appsv1alpha1.CloneSetStatus{
					Replicas:                 1,
					ReadyReplicas:            1,
					AvailableReplicas:        1,
					UpdatedReplicas:          1,
					UpdatedReadyReplicas:     1,
					UpdatedAvailableReplicas: 0,
					ExpectedUpdatedReplicas:  1,
					UpdateRevision:           "2",
					CurrentRevision:          "2",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(1, 1, 1, 1, 1, 0, 1, "2", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPaused),
				Message:            "CloneSet is paused",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with nil condition, CurrentRevision equals to UpdateRevision but CloneSet is not available and resumed",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{Replicas: ptr.To(int32(1)), ProgressDeadlineSeconds: progressDeadlineSeconds},
				Status: appsv1alpha1.CloneSetStatus{
					Replicas:                 1,
					ReadyReplicas:            0,
					AvailableReplicas:        0,
					UpdatedReplicas:          1,
					UpdatedReadyReplicas:     0,
					UpdatedAvailableReplicas: 0,
					ExpectedUpdatedReplicas:  1,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(1, 0, 0, 1, 0, 0, 1, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet is progressing",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: 11 * time.Second,
		},
		{
			name: "startup with nil condition, and CloneSet is paused due to partition available",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(10)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy:          appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstr.FromString("90%"))},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Replicas:                 10,
					ReadyReplicas:            9,
					AvailableReplicas:        9,
					UpdatedReplicas:          1,
					UpdatedReadyReplicas:     0,
					UpdatedAvailableReplicas: 0,
					ExpectedUpdatedReplicas:  1,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(10, 10, 10, 1, 1, 1, 1, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
				Message:            "CloneSet has paused due to partition ready",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with CloneSetAvailable condition, and CloneSet scales up replicas",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{ProgressDeadlineSeconds: progressDeadlineSeconds, Replicas: ptr.To(int32(10))},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetAvailable),
							Message:            "CloneSet is available",
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 10)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 10)),
						},
					},
					CurrentRevision: "1",
					UpdateRevision:  "1",
					Replicas:        5,
				},
			},
			newStatus: newStatus(5, 0, 0, 0, 0, 0, 0, "1", "1"),
			nowFn:     func() time.Time { return timeFn(0, 0) },
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetAvailable),
				Message:            "CloneSet is available",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 10)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 10)),
			},
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with CloneSetAvailable condition, and CloneSet reconciles with a new revision",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{ProgressDeadlineSeconds: progressDeadlineSeconds},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:      []appsv1alpha1.CloneSetCondition{{Type: appsv1alpha1.CloneSetConditionTypeProgressing, Reason: string(appsv1alpha1.CloneSetAvailable)}},
					CurrentRevision: "1",
					UpdateRevision:  "1",
					Replicas:        1,
				},
			},
			newStatus: newStatus(1, 0, 0, 0, 0, 0, 0, "1", "2"),
			nowFn:     func() time.Time { return timeFn(0, 0) },
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet is progressing due to revision changed from 1 to 2",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: 11 * time.Second,
		},
		{
			name: "startup with Updated condition, and CloneSet is paused due to partition available",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(10)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy:          appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstr.FromString("50%"))},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:               []appsv1alpha1.CloneSetCondition{{Type: appsv1alpha1.CloneSetConditionTypeProgressing, Reason: string(appsv1alpha1.CloneSetProgressUpdated)}},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(10, 10, 10, 5, 5, 5, 5, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
				Message:            "CloneSet has paused due to partition ready",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: time.Duration(-1),
		},
		{
			name: "startup with Updated condition, and CloneSet is scaled down from 10 to 6",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(6)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy:          appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstr.FromString("50%"))},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:               []appsv1alpha1.CloneSetCondition{{Type: appsv1alpha1.CloneSetConditionTypeProgressing, Reason: string(appsv1alpha1.CloneSetProgressUpdated)}},
					Replicas:                 6,
					ReadyReplicas:            6,
					AvailableReplicas:        6,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     5,
					UpdatedAvailableReplicas: 5,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 3, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet scale down due to expected updated replicas changes from 5 to 3",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
			expectEnqueue: 11 * time.Second,
		},
		{
			name: "startup with Updated condition, and CloneSet is scaled up from 10 to 20",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy:          appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstr.FromString("50%"))},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions:               []appsv1alpha1.CloneSetCondition{{Type: appsv1alpha1.CloneSetConditionTypeProgressing, Reason: string(appsv1alpha1.CloneSetProgressUpdated)}},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 0) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet scale up due to expected updated replicas changes from 5 to 10",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
			},
		},
		{
			name: "startup with Updated condition, and CloneSet is timeout",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy:          appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstr.FromString("50%"))},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 30) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionFalse,
				Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
				Message:            "CloneSet revision 2 has timed out progressing",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 30)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 30)),
			},
		},
		{
			name: "startup with Updated condition, and CloneSet is available",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("0%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 10)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 10)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 18) },
			newStatus: newStatus(20, 20, 20, 20, 20, 20, 20, "2", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetAvailable),
				Message:            "CloneSet is available",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 18)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 18)),
			},
		},
		{
			name: "startup with Updated condition, and CloneSet is paused",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
						Paused:    true,
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPaused),
				Message:            "CloneSet is paused",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with Paused condition, and CloneSet is paused again",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
						Paused:    true,
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressPaused),
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
							Message:            "CloneSet is paused",
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPaused),
				Message:            "CloneSet is paused",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with Paused condition, and CloneSet is resumed",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressPaused),
							Status:             v1.ConditionTrue,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 10) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet is resumed",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 10)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 10)),
			},
		},
		{
			name: "startup with DeadlineExceeded condition, and CloneSet is still progressing",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
							Message:            "CloneSet revision 2 has timed out progressing",
							Status:             v1.ConditionFalse,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 40) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
				Message:            "CloneSet revision 2 has timed out progressing",
				Status:             v1.ConditionFalse,
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with DeadlineExceeded condition, and CloneSet is paused",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
						Paused:    true,
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
							Message:            "CloneSet revision 2 has timed out progressing",
							Status:             v1.ConditionFalse,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 40) },
			newStatus: newStatus(15, 14, 14, 10, 10, 9, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
				Message:            "CloneSet revision 2 has timed out progressing",
				Status:             v1.ConditionFalse,
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with DeadlineExceeded condition, and CloneSet starts a new revision",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("90%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Reason:             string(appsv1alpha1.CloneSetProgressDeadlineExceeded),
							Message:            "CloneSet revision 2 has timed out progressing",
							Status:             v1.ConditionFalse,
							LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            5,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     3,
					UpdatedAvailableReplicas: 1,
					ExpectedUpdatedReplicas:  10,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 40) },
			newStatus: newStatus(20, 10, 10, 2, 2, 2, 2, "1", "3"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet is progressing due to revision changed from 2 to 3",
				Status:             v1.ConditionTrue,
				LastUpdateTime:     metav1.NewTime(timeFn(0, 40)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 40)),
			},
		},
		{
			name: "startup with PartitionAvailable condition, and CloneSet is paused",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(10)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
						Paused:    true,
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Status:             v1.ConditionTrue,
							Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
							Message:            "CloneSet has paused due to partition ready",
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            10,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     5,
					UpdatedAvailableReplicas: 5,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(10, 10, 10, 5, 5, 5, 5, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPaused),
				Message:            "CloneSet is paused",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with PartitionAvailable condition, and CloneSet scales up",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Status:             v1.ConditionTrue,
							Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
							Message:            "CloneSet has paused due to partition ready",
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            10,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     5,
					UpdatedAvailableReplicas: 5,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(15, 15, 15, 6, 6, 6, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressUpdated),
				Message:            "CloneSet scale up due to expected updated replicas changes from 5 to 10",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with PartitionAvailable condition, and CloneSet scales up and available again",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(20)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Status:             v1.ConditionTrue,
							Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
							Message:            "CloneSet has paused due to partition ready",
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            10,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     5,
					UpdatedAvailableReplicas: 5,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(20, 20, 20, 10, 10, 10, 10, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
				Message:            "CloneSet has paused due to partition ready",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
		{
			name: "startup with PartitionAvailable condition, and CloneSet is paused",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:                ptr.To(int32(10)),
					ProgressDeadlineSeconds: progressDeadlineSeconds,
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{
						Partition: util.GetIntOrStrPointer(intstr.FromString("50%")),
						Paused:    true,
					},
				},
				Status: appsv1alpha1.CloneSetStatus{
					Conditions: []appsv1alpha1.CloneSetCondition{
						{
							Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
							Status:             v1.ConditionTrue,
							Reason:             string(appsv1alpha1.CloneSetProgressPartitionAvailable),
							Message:            "CloneSet has paused due to partition ready",
							LastUpdateTime:     metav1.NewTime(timeFn(0, 0)),
							LastTransitionTime: metav1.NewTime(timeFn(0, 0)),
						},
					},
					Replicas:                 10,
					ReadyReplicas:            10,
					AvailableReplicas:        5,
					UpdatedReplicas:          5,
					UpdatedReadyReplicas:     5,
					UpdatedAvailableReplicas: 5,
					ExpectedUpdatedReplicas:  5,
					UpdateRevision:           "2",
					CurrentRevision:          "1",
				},
			},
			nowFn:     func() time.Time { return timeFn(0, 8) },
			newStatus: newStatus(10, 10, 10, 5, 5, 5, 5, "1", "2"),
			wantCond: &appsv1alpha1.CloneSetCondition{
				Type:               appsv1alpha1.CloneSetConditionTypeProgressing,
				Status:             v1.ConditionTrue,
				Reason:             string(appsv1alpha1.CloneSetProgressPaused),
				Message:            "CloneSet is paused",
				LastUpdateTime:     metav1.NewTime(timeFn(0, 8)),
				LastTransitionTime: metav1.NewTime(timeFn(0, 8)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nowFn = tt.nowFn
			r := &realStatusUpdater{}
			requeueDuration := r.syncProgressingStatus(tt.cs, tt.newStatus)

			cond := clonesetutils.GetCloneSetCondition(*tt.newStatus, appsv1alpha1.CloneSetConditionTypeProgressing)
			assert.Equal(t, tt.wantCond, cond)
			assert.Equal(t, tt.expectEnqueue, requeueDuration)
		})
	}
}
