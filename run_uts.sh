#!/bin/bash
PYTHONPATH=..:.:private python3 -m pytest -s -vv $(find -name "ut_*.py" -not -path "*/pylibcommons/*")
