#!/bin/bash

setup metacat
setup justin
setup rucio

export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune

MQL_REQUEST_PDSP_RAW="files from dune:all where core.file_type=detector and core.data_tier=raw and 5387 in core.runs limit 1"
MQL_REQUEST_PDHD_RAW="files from dune:all where core.file_type=detector and core.data_tier=raw and 22949 in core.runs limit 1"
MQL_REQUEST_PDSP_MC="files from dune:all where core.file_type=mc and core.data_tier=full-reconstructed and art.run_type=protodune-sp and dune.requestid=RITM1115963 and mc.space_charge=yes and 18800650 in core.runs and core.first_event_number=321"
