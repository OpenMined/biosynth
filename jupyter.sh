#!/bin/bash
uv venv --clear
uv pip install -U jupyter pandas pytest pysam
uv pip install -e ./python
source .venv/bin/activate
jupyter lab
