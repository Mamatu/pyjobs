#!/bin/bash
ARG="$@"
PYTHONPATH=..:.:private python3 -m pytest -s -vv $ARG
