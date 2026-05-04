// Copyright (c) KAITO authors.
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

package karpenter

import (
	"context"
	"errors"
	"testing"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/nodeprovision"
	"github.com/kaito-project/kaito/pkg/utils/consts"
	"github.com/kaito-project/kaito/pkg/utils/test"
)

var testConfig = NodeClassConfig{
	Group:        "karpenter.azure.com",
	Kind:         "AKSNodeClass",
	Version:      "v1beta1",
	ResourceName: "aksnodeclasses",
	DefaultName:  "image-family-ubuntu",
}

// mockNodeClassReady sets up a mock Get call for an unstructured NodeClass that
// populates the object with a Ready=True condition.
func mockNodeClassReady(mockClient *test.MockClient, name string) {
	mockClient.On("Get", mock.IsType(context.Background()), mock.MatchedBy(func(key client.ObjectKey) bool {
		return key.Name == name
	}), mock.IsType(&unstructured.Unstructured{}), mock.Anything).
		Run(func(args mock.Arguments) {
			obj := args.Get(2).(*unstructured.Unstructured)
			obj.Object = map[string]interface{}{
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
						},
					},
				},
			}
		}).
		Return(nil)
}

// mockEmptyNodeList sets up an empty NodeList for List calls.
func mockEmptyNodeList(mockClient *test.MockClient) {
	mockClient.CreateMapWithType(&corev1.NodeList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
}

// mockEmptyLegacyNodeClaims sets up an empty NodeClaimList for legacy NodeClaim List calls.
func mockEmptyLegacyNodeClaims(mockClient *test.MockClient) {
	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)
}

// mockNodePoolNotFound sets up a Get call for NodePool that returns NotFound.
func mockNodePoolNotFound(mockClient *test.MockClient, name string) {
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, name)
	mockClient.On("Get", mock.IsType(context.Background()), mock.MatchedBy(func(key client.ObjectKey) bool {
		return key.Name == name
	}), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(notFoundErr)
}

// makeReadyNode creates a ready Node with the given name, instance type, and extra labels.
func makeReadyNode(name, instanceType string, extraLabels map[string]string) *corev1.Node {
	labels := map[string]string{
		"apps":                         "llm",
		corev1.LabelInstanceTypeStable: instanceType,
	}
	for k, v := range extraLabels {
		labels[k] = v
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestKarpenterProvisionerImplementsInterface(t *testing.T) {
	mockClient := test.NewClient()
	var _ nodeprovision.NodeProvisioner = NewKarpenterProvisioner(mockClient, testConfig)
}

func TestName(t *testing.T) {
	p := NewKarpenterProvisioner(test.NewClient(), testConfig)
	assert.Equal(t, "KarpenterProvisioner", p.Name())
}

func TestStart_CRDNotFound(t *testing.T) {
	mockClient := test.NewClient()
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, "aksnodeclasses.karpenter.azure.com")
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&apiextensionsv1.CustomResourceDefinition{}), mock.Anything).Return(notFoundErr)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.Start(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Karpenter must be installed")
}

// --- ProvisionNodes tests ---

func TestProvisionNodes_NoBYONodes_CreatesWithFullReplicas(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)
	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	// Verify replicas = targetNodeCount (2) since no BYO nodes.
	for _, call := range mockClient.Calls {
		if call.Method == "Create" {
			if np, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, int64(2), *np.Spec.Replicas)
			}
		}
	}
}

func TestProvisionNodes_DeltaWithBYONodes(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")

	// 1 BYO node exists (ready, correct instance type, no karpenter label).
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil))] =
		makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil)
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
	mockEmptyLegacyNodeClaims(mockClient)

	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	// desiredReplicas = 2 - 1 = 1
	for _, call := range mockClient.Calls {
		if call.Method == "Create" {
			if np, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, int64(1), *np.Spec.Replicas)
			}
		}
	}
}

func TestProvisionNodes_KarpenterNodesExcludedFromDelta(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")

	// 1 karpenter node (has workspace-name label — excluded from non-karpenter count).
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	karpenterNode := makeReadyNode("karpenter-1", "Standard_NC24ads_A100_v4", map[string]string{
		consts.KarpenterWorkspaceNameKey: "ws1",
	})
	nodeMap[client.ObjectKeyFromObject(karpenterNode)] = karpenterNode
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
	mockEmptyLegacyNodeClaims(mockClient)

	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	// desiredReplicas = 2 (karpenter node excluded from non-karpenter count).
	for _, call := range mockClient.Calls {
		if call.Method == "Create" {
			if np, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, int64(2), *np.Spec.Replicas)
			}
		}
	}
}

func TestProvisionNodes_ZeroReplicasWhenBYOSufficient_NoCreateCalled(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")

	// 2 BYO nodes, target=2 → desiredReplicas=0, no NodePool should be created.
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	for _, name := range []string{"byo-1", "byo-2"} {
		node := makeReadyNode(name, "Standard_NC24ads_A100_v4", nil)
		nodeMap[client.ObjectKeyFromObject(node)] = node
	}
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
	mockEmptyLegacyNodeClaims(mockClient)

	mockNodePoolNotFound(mockClient, "default-ws1")

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)
	mockClient.AssertNotCalled(t, "Create", mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything)
}

func TestProvisionNodes_DoesNotDecreaseReplicas(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")

	// 1 BYO node appeared after NodePool was created with replicas=2.
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil))] =
		makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil)
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)
	mockEmptyLegacyNodeClaims(mockClient)

	// NodePool exists with replicas=2.
	existingNP := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec:       karpenterv1.NodePoolSpec{Replicas: lo.ToPtr(int64(2))},
	}
	mockClient.CreateOrUpdateObjectInMap(existingNP)
	mockClient.On("Get", mock.IsType(context.Background()), mock.MatchedBy(func(key client.ObjectKey) bool {
		return key.Name == "default-ws1"
	}), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)
	// desiredReplicas=1 < currentReplicas=2 → no update.
	mockClient.AssertNotCalled(t, "Update", mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything)
}

func TestProvisionNodes_IncreasesReplicas(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)

	// NodePool exists with replicas=1, target=3, no BYO → desired=3 > current=1, should update.
	existingNP := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec:       karpenterv1.NodePoolSpec{Replicas: lo.ToPtr(int64(1))},
	}
	mockClient.CreateOrUpdateObjectInMap(existingNP)
	mockClient.On("Get", mock.IsType(context.Background()), mock.MatchedBy(func(key client.ObjectKey) bool {
		return key.Name == "default-ws1"
	}), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)
	mockClient.On("Update", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 3, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	for _, call := range mockClient.Calls {
		if call.Method == "Update" {
			if np, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, int64(3), *np.Spec.Replicas)
			}
		}
	}
}

func TestProvisionNodes_NoUpdateWhenReplicasUnchanged(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)

	// NodePool exists with replicas=2, target=2, no BYO → desired=2, no update needed.
	existingNP := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec:       karpenterv1.NodePoolSpec{Replicas: lo.ToPtr(int64(2))},
	}
	mockClient.CreateOrUpdateObjectInMap(existingNP)
	mockClient.On("Get", mock.IsType(context.Background()), mock.MatchedBy(func(key client.ObjectKey) bool {
		return key.Name == "default-ws1"
	}), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)
	mockClient.AssertNotCalled(t, "Update", mock.Anything, mock.Anything, mock.Anything)
}

func TestProvisionNodes_AlreadyExists(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)
	mockNodePoolNotFound(mockClient, "default-ws1")
	alreadyExistsErr := apierrors.NewAlreadyExists(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(alreadyExistsErr)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err) // AlreadyExists is silently ignored
}

func TestProvisionNodes_CreateError(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)
	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(errors.New("API server down"))

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "API server down")
}

func TestProvisionNodes_UsesDefaultNodeClassName(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-ubuntu")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)
	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	for _, call := range mockClient.Calls {
		if call.Method == "Create" {
			if createdNP, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, "image-family-ubuntu", createdNP.Spec.Template.Spec.NodeClassRef.Name)
			}
		}
	}
}

func TestProvisionNodes_UsesAnnotationNodeClassName(t *testing.T) {
	mockClient := test.NewClient()
	mockNodeClassReady(mockClient, "image-family-azure-linux")
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)
	mockNodePoolNotFound(mockClient, "default-ws1")
	mockClient.On("Create", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, map[string]string{
		kaitov1beta1.AnnotationNodeClassName: "image-family-azure-linux",
	})

	err := p.ProvisionNodes(context.Background(), ws)
	assert.NoError(t, err)

	for _, call := range mockClient.Calls {
		if call.Method == "Create" {
			if createdNP, ok := call.Arguments[1].(*karpenterv1.NodePool); ok {
				assert.Equal(t, "image-family-azure-linux", createdNP.Spec.Template.Spec.NodeClassRef.Name)
			}
		}
	}
}

// --- DeleteNodes tests ---

func TestDeleteNodes_Success(t *testing.T) {
	mockClient := test.NewClient()
	np := &karpenterv1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"}}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)
	mockClient.On("Delete", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.DeleteNodes(context.Background(), ws)
	assert.NoError(t, err)
}

func TestDeleteNodes_NotFound(t *testing.T) {
	mockClient := test.NewClient()
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, "default-ws1")
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(notFoundErr)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.DeleteNodes(context.Background(), ws)
	assert.NoError(t, err) // NotFound is silently ignored
}

func TestDeleteNodes_AlreadyDeleting(t *testing.T) {
	mockClient := test.NewClient()
	now := metav1.Now()
	np := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "default-ws1",
			DeletionTimestamp: &now,
			Finalizers:        []string{"karpenter.sh/termination"},
		},
	}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.DeleteNodes(context.Background(), ws)
	assert.NoError(t, err)
	// Delete should NOT have been called
	mockClient.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything, mock.Anything)
}

func TestDeleteNodes_DeleteError(t *testing.T) {
	mockClient := test.NewClient()
	np := &karpenterv1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"}}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)
	mockClient.On("Delete", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(errors.New("forbidden"))

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	err := p.DeleteNodes(context.Background(), ws)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "forbidden")
}

// --- EnableDriftRemediation tests ---

func TestEnableDriftRemediation_Success(t *testing.T) {
	mockClient := test.NewClient()

	np := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec: karpenterv1.NodePoolSpec{
			Disruption: karpenterv1.Disruption{
				ConsolidateAfter: karpenterv1.MustParseNillableDuration("0s"),
				Budgets: []karpenterv1.Budget{
					{
						Nodes:   "0",
						Reasons: []karpenterv1.DisruptionReason{karpenterv1.DisruptionReasonDrifted},
					},
				},
			},
		},
	}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)
	mockClient.On("Update", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.EnableDriftRemediation(context.Background(), "default", "ws1")
	assert.NoError(t, err)

	found := false
	for _, call := range mockClient.Calls {
		if call.Method == "Update" {
			updatedNP := call.Arguments[1].(*karpenterv1.NodePool)
			assert.Equal(t, "1", updatedNP.Spec.Disruption.Budgets[0].Nodes)
			found = true
		}
	}
	assert.True(t, found, "expected Update to be called")
}

func TestEnableDriftRemediation_NodePoolNotFound(t *testing.T) {
	mockClient := test.NewClient()
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, "default-ws1")
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(notFoundErr)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.EnableDriftRemediation(context.Background(), "default", "ws1")
	assert.Error(t, err)
}

func TestEnableDriftRemediation_NoDriftedBudgetEntry(t *testing.T) {
	mockClient := test.NewClient()

	np := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec: karpenterv1.NodePoolSpec{
			Disruption: karpenterv1.Disruption{
				ConsolidateAfter: karpenterv1.MustParseNillableDuration("0s"),
				Budgets: []karpenterv1.Budget{
					{
						Nodes:   "10%",
						Reasons: []karpenterv1.DisruptionReason{karpenterv1.DisruptionReasonEmpty},
					},
				},
			},
		},
	}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.EnableDriftRemediation(context.Background(), "default", "ws1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no budget entry with Drifted reason")
}

// --- DisableDriftRemediation tests ---

func TestDisableDriftRemediation_Success(t *testing.T) {
	mockClient := test.NewClient()

	np := &karpenterv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ws1"},
		Spec: karpenterv1.NodePoolSpec{
			Disruption: karpenterv1.Disruption{
				ConsolidateAfter: karpenterv1.MustParseNillableDuration("0s"),
				Budgets: []karpenterv1.Budget{
					{
						Nodes:   "1",
						Reasons: []karpenterv1.DisruptionReason{karpenterv1.DisruptionReasonDrifted},
					},
				},
			},
		},
	}
	mockClient.CreateOrUpdateObjectInMap(np)
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)
	mockClient.On("Update", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.DisableDriftRemediation(context.Background(), "default", "ws1")
	assert.NoError(t, err)

	found := false
	for _, call := range mockClient.Calls {
		if call.Method == "Update" {
			updatedNP := call.Arguments[1].(*karpenterv1.NodePool)
			assert.Equal(t, "0", updatedNP.Spec.Disruption.Budgets[0].Nodes)
			found = true
		}
	}
	assert.True(t, found, "expected Update to be called")
}

func TestDisableDriftRemediation_NodePoolNotFound(t *testing.T) {
	mockClient := test.NewClient()
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, "default-ws1")
	mockClient.On("Get", mock.IsType(context.Background()), mock.Anything, mock.IsType(&karpenterv1.NodePool{}), mock.Anything).Return(notFoundErr)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	err := p.DisableDriftRemediation(context.Background(), "default", "ws1")
	assert.Error(t, err)
}

// --- EnsureNodesReady tests ---

func TestEnsureNodesReady_AllReady_BYOOnly(t *testing.T) {
	mockClient := test.NewClient()

	// 1 ready BYO node with correct instance type.
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil))] =
		makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil)
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

	// No karpenter NodeClaims.
	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	ready, needRequeue, err := p.EnsureNodesReady(context.Background(), ws)
	assert.NoError(t, err)
	assert.True(t, ready)
	assert.False(t, needRequeue)
}

func TestEnsureNodesReady_MixedBYOAndKarpenter(t *testing.T) {
	mockClient := test.NewClient()

	// 1 BYO + 1 karpenter node, target=2.
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	byoNode := makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil)
	karpNode := makeReadyNode("karp-1", "Standard_NC24ads_A100_v4", map[string]string{
		consts.KarpenterWorkspaceNameKey: "ws1",
	})
	nodeMap[client.ObjectKeyFromObject(byoNode)] = byoNode
	nodeMap[client.ObjectKeyFromObject(karpNode)] = karpNode
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

	// 1 karpenter NodeClaim ready.
	nc := &karpenterv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "nc-1",
			Labels: map[string]string{karpenterv1.NodePoolLabelKey: "default-ws1"},
		},
		Status: karpenterv1.NodeClaimStatus{
			Conditions: []status.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	ncList := &karpenterv1.NodeClaimList{}
	ncMap := mockClient.CreateMapWithType(ncList)
	ncMap[client.ObjectKeyFromObject(nc)] = nc
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	ready, needRequeue, err := p.EnsureNodesReady(context.Background(), ws)
	assert.NoError(t, err)
	assert.True(t, ready)
	assert.False(t, needRequeue)
}

func TestEnsureNodesReady_CountBelowTarget(t *testing.T) {
	mockClient := test.NewClient()

	// No karpenter NodeClaims.
	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 2, nil, nil)

	ready, needRequeue, err := p.EnsureNodesReady(context.Background(), ws)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.False(t, needRequeue)
}

func TestEnsureNodesReady_ListError(t *testing.T) {
	mockClient := test.NewClient()
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(errors.New("API error"))

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	_, _, err := p.EnsureNodesReady(context.Background(), ws)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestEnsureNodesReady_DeletingNodeNotCounted(t *testing.T) {
	mockClient := test.NewClient()

	// No karpenter NodeClaims.
	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	// 1 node being deleted — should not count.
	now := metav1.Now()
	deletingNode := makeReadyNode("deleting-1", "Standard_NC24ads_A100_v4", nil)
	deletingNode.DeletionTimestamp = &now
	deletingNode.Finalizers = []string{"test/finalizer"}
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(deletingNode)] = deletingNode
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	ready, needRequeue, err := p.EnsureNodesReady(context.Background(), ws)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.False(t, needRequeue)
}

// --- CollectNodeStatusInfo tests ---

func TestCollectNodeStatusInfo_AllReady_BYOOnly(t *testing.T) {
	mockClient := test.NewClient()

	// 1 ready BYO node.
	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil))] =
		makeReadyNode("byo-1", "Standard_NC24ads_A100_v4", nil)
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

	// No karpenter NodeClaims.
	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	conditions, err := p.CollectNodeStatusInfo(context.Background(), ws)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(conditions))

	for _, cond := range conditions {
		assert.Equal(t, metav1.ConditionTrue, cond.Status, "condition %s should be True", cond.Type)
	}
}

func TestCollectNodeStatusInfo_NotEnoughNodes(t *testing.T) {
	mockClient := test.NewClient()
	mockEmptyNodeList(mockClient)
	mockEmptyLegacyNodeClaims(mockClient)

	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	conditions, err := p.CollectNodeStatusInfo(context.Background(), ws)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(conditions))

	for _, cond := range conditions {
		assert.Equal(t, metav1.ConditionFalse, cond.Status, "condition %s should be False", cond.Type)
	}
}

func TestCollectNodeStatusInfo_ListError(t *testing.T) {
	mockClient := test.NewClient()
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(errors.New("API error"))

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	_, err := p.CollectNodeStatusInfo(context.Background(), ws)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestCollectNodeStatusInfo_ConditionTypes(t *testing.T) {
	mockClient := test.NewClient()

	nodeList := &corev1.NodeList{}
	nodeMap := mockClient.CreateMapWithType(nodeList)
	nodeMap[client.ObjectKeyFromObject(makeReadyNode("node-1", "Standard_NC24ads_A100_v4", nil))] =
		makeReadyNode("node-1", "Standard_NC24ads_A100_v4", nil)
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&corev1.NodeList{}), mock.Anything).Return(nil)

	mockClient.CreateMapWithType(&karpenterv1.NodeClaimList{})
	mockClient.On("List", mock.IsType(context.Background()), mock.IsType(&karpenterv1.NodeClaimList{}), mock.Anything).Return(nil)

	p := NewKarpenterProvisioner(mockClient, testConfig)
	ws := newTestWorkspace("default", "ws1", "Standard_NC24ads_A100_v4", 1, nil, nil)

	conditions, err := p.CollectNodeStatusInfo(context.Background(), ws)
	assert.NoError(t, err)

	typeSet := map[string]bool{}
	for _, cond := range conditions {
		typeSet[cond.Type] = true
	}
	assert.True(t, typeSet[string(kaitov1beta1.ConditionTypeNodeStatus)])
	assert.True(t, typeSet[string(kaitov1beta1.ConditionTypeNodeClaimStatus)])
	assert.True(t, typeSet[string(kaitov1beta1.ConditionTypeResourceStatus)])
}
