install: install-go-tools

install-go-tools:
	go install golang.org/x/tools/cmd/goimports@v0.20.0;
	go install gotest.tools/gotestsum@v1.11.0;


fmt:     ## Run "go fmt" on the entire gortia
	go fmt $(shell go list ./...)

vet:     ## Run "go vet" on the entire gortia
	go vet $(shell go list ./...)

gen-go:
	go generate ./...

test:    ## Run all the tests in the project except for integration-tests
	gotestsum  --format-hide-empty-pkg -- -v -race -short ./...
test-apple:    ## apple silicon warning temporary fix:
	gotestsum  --format-hide-empty-pkg -- -v -race -short -ldflags=-extldflags=-Wl,-ld_classic ./...

test-all:    ## Run all the tests in the project including integration-tests
	gotestsum --format-hide-empty-pkg -- -tags=integration -v -race ./...

check: fmt vet test  ## Run fmt, vet and test
