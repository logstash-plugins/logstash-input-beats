#!/bin/bash

if [ $(command -v apt) ]; then
    sudo apt install -y openssl
else
    sudo microdnf install -y openssl
fi
