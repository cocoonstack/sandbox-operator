package sandboxoperator

//go:generate go tool -modfile=tools.mod sigs.k8s.io/controller-tools/cmd/controller-gen object crd:maxDescLen=0 paths=./api/... output:crd:dir=helm/crds
