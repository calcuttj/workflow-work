#!/bin/bash
echo "Posix preload: ${xrootd_posix_preload}"
LD_PRELOAD=$xrootd_posix_preload lardon -file ${1} -out ${2} -trk -n 1