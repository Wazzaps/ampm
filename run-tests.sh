#!/bin/sh

set -ex

PYTHONPATH='vendor:.' pytest tests/ -rP "$@"