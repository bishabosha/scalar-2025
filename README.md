# scalar-2025
Ideas and demonstrations of named tuples to the max

## Setup

required tools:
- `scala` command (>3.5.0)
- `make`

run the following to publish all the sub projects locally:
- `scala setup.sc` (if this doesn't work try with the `scala-cli` command)

then to run the final webapp:
```shell
cd full-stack-application
make packageJs # bundles the frontend
make serve # serve backend with log
```
which should serve at http://localhost:8080
