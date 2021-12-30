#!/bin/bash

if [ $(command -v apt) ]; then
    sudo apt install -y openssl
else
    sudo yum install -y openssl
fi