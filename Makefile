PYTHON ?= python3
.PHONY: build test test-race test-integration lint up down seed generate-events benchmark load-test replay-test failure-test helm-lint kind-deploy clean frontend e2e demo generate
build test test-race test-integration lint up down seed generate-events benchmark load-test replay-test failure-test helm-lint kind-deploy clean frontend e2e demo generate:
	$(PYTHON) scripts/tasks.py $@
