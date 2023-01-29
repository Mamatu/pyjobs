#!/bin/bash
PYTHONPATH=..:.:private python3 -m pytest -s $(find -name "ut_*.py")
