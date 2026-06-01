#!/bin/bash

set -euo pipefail

if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
else
    SUDO=""
fi

if command -v apt >/dev/null 2>&1; then
    ${SUDO} apt install -y openssl
elif command -v microdnf >/dev/null 2>&1; then
    ${SUDO} microdnf install -y openssl
else
    echo "No supported package manager found (expected apt or microdnf)" >&2
    exit 1
fi
