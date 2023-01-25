#!/usr/bin/env bash

set -ex

pip install pip-tools
pip-sync requirements.txt dev-requirements.txt

pip install -e .

pre-commit install
