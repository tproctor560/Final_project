# Set the directory holding Kubernetes YAML files for testing.
KUBE_TEST_DIR := kubernetes/test
# Namespace used in the test YAMLs (adjust if needed)
NAMESPACE := vbetala6276

.PHONY: test-k8s-up test-k8s-down test-k8s-status

# Apply all Kubernetes test resources.
test-k8s-up:
	@echo "Applying Kubernetes test resources in $(KUBE_TEST_DIR) to namespace $(NAMESPACE)..."
	kubectl apply -f $(KUBE_TEST_DIR) -n $(NAMESPACE)

# Delete all Kubernetes test resources.
test-k8s-down:
	@echo "Deleting Kubernetes test resources in $(KUBE_TEST_DIR) from namespace $(NAMESPACE)..."
	kubectl delete -f $(KUBE_TEST_DIR) -n $(NAMESPACE)

# Show the status of resources in the test namespace.
test-k8s-status:
	@echo "Listing all resources in namespace $(NAMESPACE)..."
	kubectl get all -n $(NAMESPACE)