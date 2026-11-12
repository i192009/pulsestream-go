# Contributing

Use Go 1.26 and Node 24, or the Docker-based task runner (`python scripts/tasks.py`). Read the consistency/window documentation before changing partition processing, state schemas, sink keys or replay behavior. Add an executable test for any change to acknowledgement, offset, checkpoint, deduplication or finality semantics.

Run `python scripts/tasks.py test-race`, `test-integration`, `frontend`, `lint` and `helm-lint`. Start the stack and run `e2e --count 1000` plus `failure-test` for distributed behavior changes. Record actual results; opt-in tests skipped without dependencies are not evidence of an integration pass. Regenerate protobufs with `python scripts/tasks.py generate`. Do not check in secrets, build artifacts or machine-specific tool installations.

PR descriptions should identify the behavior change, consistency implications, executed tests and unverified deployment assumptions. Report performance using the included finite workload generator, workload parameters and hardware details. Never infer distributed throughput from the CPU window benchmark.
