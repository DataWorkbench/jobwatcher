#!/usr/bin/env bash

current_path=$(cd "$(dirname "${0}")" || exit 1; pwd)

cd "${current_path}"/.. || exit 1

BUILD="build"
PROGRAM="jobwatcher"

mkdir -p ${BUILD}/bin;

OUTPUT="./${BUILD}/bin/${PROGRAM}"

#MODULE="$(go list -mod=mod)/cmds"
MODULE="github.com/DataWorkbench/common/utils/buildinfo"

if [[ "${BUILD_MODE}" == "release" ]]; then
    TAGS="netgo ${BUILD_MODE}"
else
    TAGS="netgo"
fi

go mod tidy
go mod download

go build --tags "${TAGS}" -ldflags "
-X ${MODULE}.GoVersion=$(go version|awk '{print $3}')
-X ${MODULE}.CompileBy=$(git config user.email)
-X ${MODULE}.CompileTime=$(date '+%Y-%m-%d:%H:%M:%S')
-X ${MODULE}.GitBranch=$(git rev-parse --abbrev-ref HEAD)
-X ${MODULE}.GitCommit=$(git rev-parse --short HEAD)
-X ${MODULE}.OsArch=$(uname)/$(uname -m)
" \
-v -o ${OUTPUT} .

exit $?

