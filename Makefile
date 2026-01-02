.DEFAULT_GOAL := help

.PHONY: help
help: ## Show help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.PHONY: lint
lint: ## Run formatters and lint
	uv run ruff check --fix
	uv run ruff format
	uv run pyright

.PHONY: test
test: ## Run tests
	uv run pytest -m "not e2e"

.PHONY: test_e2e
test_e2e: ## Run E2E tests
	uv run pytest -m e2e

.PHONY: build
build: ## Build package
	uv build

.PHONY: check_dist
check_dist: ## Check distribution files
	uv run twine check  "dist/*"
