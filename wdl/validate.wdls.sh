#!/bin/bash

# we do not set eu here because the WDLs may break and we want to catch that then continue

echo "VALIDATING WDL FILES IN THE CURRENT DIRECTORY"
(womtool --version || echo "I need womtool to run" ) && echo

for wdl in *.wdl; do
  echo -e "==============================\n${wdl}";
  womtool validate "${wdl}";
done
