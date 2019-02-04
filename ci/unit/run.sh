#!/bin/bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

bundle exec rspec spec
bundle exec rake test:integration:setup
bundle exec rspec spec --tag integration
