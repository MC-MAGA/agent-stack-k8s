#!/usr/bin/env ash

set -eufo pipefail

echo --- :hammer: Installing tools
apk add --update-cache --no-progress helm git

source .buildkite/steps/repo_info.sh

# Note that the Helm command below still supplies a GraphQL token,
# so that it is available in the Helm-generated Kubernetes Secret,
# so that it is available for the integration tests.

echo --- :helm: Helm upgrade
helm upgrade agent-stack-k8s "${helm_repo_pecr}/agent-stack-k8s" \
  --version "${version}" \
  --namespace buildkite \
  --install \
  --create-namespace \
  --wait \
  -f .buildkite/production-helm-values.yaml \
  --set agentToken="${DEFAULT_CLUSTER_TOKEN}" \
  --set config.image=buildkite/agent:beta

# Note: using :beta to test v4 with agent-stack-k8s prior to v4 hitting stable.
# In case of problems with v4 or the beta image, set this back to use:
#  --set config.image="${agent_image}"
