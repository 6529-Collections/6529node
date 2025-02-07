# 6529 Core Node

[![codecov](https://codecov.io/gh/6529-Collections/6529node/branch/main/graph/badge.svg)](https://codecov.io/gh/6529-Collections/6529node)

Application that acts as 6529 Core node.

## Development

Application is developed in Golang. To build and run from the source code:

1. Install Go and make sure you have your GOPATH set and in PATH. Example `export GOPATH="$HOME/.go"` and `export PATH="$GOPATH/bin:$PATH"`.
2. Install go-task: `go install github.com/go-task/task/v3/cmd/task@latest`.
3. Make a copy of config.example.env to project root and name it config.env.
4. Modify properties in config.env as necessary.
5. Run `task install-dev-tools` and `task tidy`. These makes sure you have all the dev and app dependencies in place.
6. To run straight from the source run `task run`.
7. To run only tests run `task test`.
8. To build run `task build`. Output binary lands in `bin` folder.
9. Before commiting run `task format` and `task lint`. Those make sure your changes are correctly formatted and it statisfies all the linting rules. If any of those tasks change anything or fail in the Gihub CI, then a CI pipeline will fail and your PR can't be merged.
10. To view db entries run `task dump-db`. This will display the contents of the db in the console. You can also run `task dump-db -- -o file` to dump to a file `dump.txt` by default or `task dump-db -- -o file -f <filename>` to dump to specific file
