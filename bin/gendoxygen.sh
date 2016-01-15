#!/usr/bin/env bash
if [ ! -f run.py ]; then
	echo "run from root of project!"
	exit 1
fi
mkdir -p docs
doxygen config/Doxyfile
