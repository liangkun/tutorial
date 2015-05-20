#!/bin/bash

# find sbt, download it if nessary.
function sbt_executable() {
    local sbt="$(which sbt)"
    if [ -z "$sbt" ]; then
        sbt="$(prepare_sbt)"
    fi
    echo "$sbt"
}

# download sbt
function prepare_sbt() {
    local cwd="$(pwd)"
    wget "https://dl.bintray.com/sbt/native-packages/sbt/0.13.8/sbt-0.13.8.tgz"
    tar -zxf sbt-0.13.8.tgz
    rm -f sbt-0.13.8.tgz
    echo "$cwd/sbt/bin/sbt"
}

if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME not set"
    exit 1
fi

sbt="$(sbt_executable)"

"$sbt" clean assembly
