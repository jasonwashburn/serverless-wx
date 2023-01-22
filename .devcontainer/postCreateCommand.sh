#!/usr/bin/env bash

set -ex

pip install pip-tools
pip-sync requirements.txt dev-requirements.txt

pre-commit install
