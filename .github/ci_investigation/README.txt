Temporary CI performance investigation, based on main 552703312773cd525c0b74ce6bcb1b8737b44d00.

Do not merge this branch: tests.yml is deliberately replaced with a manually
dispatched diagnostic workflow to avoid running the production matrix while
measuring timeout and worker-crash behavior. Application source and existing
tests are unchanged. No pull request is intended.

The runner saves per-process stack dumps, phase events, process resource
samples, JUnit reports and Python profiles. Expected timeouts and test failures
are recorded as experiment results; a green diagnostic job does not mean its
child test invocations passed. Each child has an external process-tree watchdog.
