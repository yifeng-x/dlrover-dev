// Copyright 2022 The DLRover Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"strings"
	"testing"

	elasticv1alpha1 "github.com/intelligent-machine-learning/dlrover/go/elasticjob/api/v1alpha1"
	common "github.com/intelligent-machine-learning/dlrover/go/elasticjob/pkg/common"
	commonv1 "github.com/intelligent-machine-learning/dlrover/go/elasticjob/pkg/common/api/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateMasterPod(t *testing.T) {
	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ps",
			Namespace: "dlrover",
			Annotations: map[string]string{
				"pending_timeout":       "300",
				"service_type":          "http",
				"pre_check_ops":         "[]",
				"training_elastic_mode": "ucp",
			},
			Labels: map[string]string{},
		},
	}
	job.Spec.ReplicaSpecs = make(map[commonv1.ReplicaType]*elasticv1alpha1.ReplicaSpec)
	SetDefaultMasterTemplateToJob(job, "dlrover-master:test")
	pod := newElasticJobMaster(job, int32(0))
	assert.Equal(t, pod.Name, "elasticjob-test-ps-dlrover-master-0")
	assert.Equal(t, pod.Spec.Containers[0].Image, "dlrover-master:test")
	assert.Equal(t, "dlrover-master", pod.Labels[common.LabelReplicaTypeKey])
	assert.Equal(t, string(pod.Spec.Containers[0].ImagePullPolicy), "Always")
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--namespace dlrover"))
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--job_name test-ps"))
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--port 50001"))
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--metrics_port 9090"))
	assert.Equal(t, int32(9090), pod.Spec.Containers[0].Ports[0].ContainerPort)
	assert.Equal(t, "metrics", pod.Spec.Containers[0].Ports[0].Name)
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--pending_timeout 300"))
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--service_type http"))
	assert.True(t, strings.Contains(pod.Spec.Containers[0].Command[2], "--training_elastic_mode ucp"))
}

func TestCreateMasterPodWithImage(t *testing.T) {
	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-ps",
			Namespace:   "dlrover",
			Annotations: map[string]string{},
			Labels:      map[string]string{},
		},
	}

	container := corev1.Container{
		Image:           "dlrover-master:test-v0",
		ImagePullPolicy: "Always",
	}
	job.Spec.ReplicaSpecs = make(map[commonv1.ReplicaType]*elasticv1alpha1.ReplicaSpec)
	job.Spec.ReplicaSpecs[JobMasterReplicaType] = &elasticv1alpha1.ReplicaSpec{
		ReplicaSpec: commonv1.ReplicaSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers:    []corev1.Container{container},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	SetDefaultMasterTemplateToJob(job, "dlrover-master:test")
	pod := newElasticJobMaster(job, int32(0))
	assert.Equal(t, pod.Name, "elasticjob-test-ps-dlrover-master-0")
	assert.Equal(t, pod.Spec.Containers[0].Image, "dlrover-master:test-v0")
	assert.Equal(t, string(pod.Spec.Containers[0].ImagePullPolicy), "Always")
	assert.Len(t, pod.Spec.Containers[0].Ports, 1)
	assert.Equal(t, "metrics", pod.Spec.Containers[0].Ports[0].Name)
	assert.Equal(t, int32(9090), pod.Spec.Containers[0].Ports[0].ContainerPort)
	assert.Equal(t, corev1.ProtocolTCP, pod.Spec.Containers[0].Ports[0].Protocol)
}

func TestCustomMasterMetricsPortIsPreserved(t *testing.T) {
	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "dlrover"},
	}
	job.Spec.ReplicaSpecs = map[commonv1.ReplicaType]*elasticv1alpha1.ReplicaSpec{
		JobMasterReplicaType: {
			ReplicaSpec: commonv1.ReplicaSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "main",
						Ports: []corev1.ContainerPort{{
							Name: "metrics", ContainerPort: 19090,
						}},
					}}},
				},
			},
		},
	}

	SetDefaultMasterTemplateToJob(job, "unused")
	ports := job.Spec.ReplicaSpecs[JobMasterReplicaType].Template.Spec.Containers[0].Ports

	assert.Len(t, ports, 1)
	assert.Equal(t, "metrics", ports[0].Name)
	assert.Equal(t, int32(19090), ports[0].ContainerPort)
	assert.Equal(t, corev1.ProtocolTCP, ports[0].Protocol)
}

func TestCreateMasterServiceWithMetricsPort(t *testing.T) {
	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "dlrover"},
	}
	service := newJobMasterService(job, newJobMasterServiceName(job.Name))

	assert.Equal(t, "test", service.Spec.Selector[common.LabelJobNameKey])
	assert.Equal(t, "dlrover-master", service.Spec.Selector[common.LabelReplicaTypeKey])
	assert.Len(t, service.Spec.Ports, 2)
	assert.Equal(t, "master", service.Spec.Ports[0].Name)
	assert.Equal(t, int32(50001), service.Spec.Ports[0].Port)
	assert.Equal(t, intstr.FromInt(50001), service.Spec.Ports[0].TargetPort)
	assert.Equal(t, "metrics", service.Spec.Ports[1].Name)
	assert.Equal(t, int32(9090), service.Spec.Ports[1].Port)
	assert.Equal(t, intstr.FromString("metrics"), service.Spec.Ports[1].TargetPort)
}

func TestReconcileUpgradesExistingMasterService(t *testing.T) {
	testScheme := runtime.NewScheme()
	assert.NoError(t, corev1.AddToScheme(testScheme))
	assert.NoError(t, elasticv1alpha1.AddToScheme(testScheme))

	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "dlrover"},
	}
	job.Spec.ReplicaSpecs = make(map[commonv1.ReplicaType]*elasticv1alpha1.ReplicaSpec)
	SetDefaultMasterTemplateToJob(job, "dlrover-master:test")
	serviceName := newJobMasterServiceName(job.Name)
	oldService := common.NewService(
		job,
		serviceName,
		masterServicePort,
		map[string]string{common.LabelReplicaTypeKey: string(JobMasterReplicaType)},
	)
	client := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(oldService).Build()

	assert.NoError(t, ReconcileJobMasterPod(client, job, 0))
	updated := &corev1.Service{}
	assert.NoError(t, client.Get(
		context.Background(),
		types.NamespacedName{Name: serviceName, Namespace: job.Namespace},
		updated,
	))
	assert.Len(t, updated.Spec.Ports, 2)
	assert.Equal(t, "master", updated.Spec.Ports[0].Name)
	assert.Equal(t, "metrics", updated.Spec.Ports[1].Name)
	assert.Equal(t, intstr.FromString("metrics"), updated.Spec.Ports[1].TargetPort)
}

func TestCreateMasterPodWithOptimizeMode(t *testing.T) {
	job := &elasticv1alpha1.ElasticJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-ps",
			Namespace:   "dlrover",
			Annotations: map[string]string{},
			Labels:      map[string]string{},
		},
	}
	job.Spec.OptimizeMode = "cluster"
	job.Spec.ReplicaSpecs = make(map[commonv1.ReplicaType]*elasticv1alpha1.ReplicaSpec)
	SetDefaultMasterTemplateToJob(job, "dlrover-master:test")
	pod := newElasticJobMaster(job, int32(0))
	assert.Equal(t, pod.Name, "elasticjob-test-ps-dlrover-master-0")
	assert.Equal(t, job.Spec.BrainService, "")
}
