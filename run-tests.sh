#!/bin/sh

PYTHONPATH='vendor:.' pytest tests/ -rP "$@"