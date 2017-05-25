#!/bin/bash
########################################################
#
# AUTOMATICALLY GENERATED! DO NOT EDIT
#
# version: 1
########################################################
set -e

echo "Starting build process in: `pwd`"
./ci/setup.sh

if [[ -f "ci/run.sh" ]]; then
    echo "Running custom build script in: `pwd`/ci/run.sh"
    ./ci/run.sh
else
    echo "Running default build scripts in: `pwd`/ci/build.sh"
    bundle install
    bundle exec rspec spec
fi
