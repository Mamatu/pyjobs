#!/bin/bash
ARG=$1
PYTHONPATH=..:.:private python3 -m pytest -s -vv $ARG
