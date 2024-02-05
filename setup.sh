#!/bin/bash

setup metacat
setup justin
setup rucio
setup dunesw v09_81_00d02 -q e26:prof

export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune

export MQL_REQUEST_PDSP_RAW="files from dune:all where core.file_type=detector and core.data_tier=raw and 5387 in core.runs limit 1"
export MQL_REQUEST_PDHD_RAW="files from dune:all where core.file_type=detector and core.data_tier=raw and 22949 in core.runs limit 1"
export MQL_REQUEST_PDSP_MC="files from dune:all where core.file_type=mc and core.data_tier=full-reconstructed and art.run_type=protodune-sp and dune.requestid=RITM1115963 and mc.space_charge=yes and 18800650 in core.runs and core.first_event_number=321"
export MQL_REQUEST_11_22_23_DR23="files from dune:all where core.run_type=hd-protodune and core.file_type=detector and DUNE.campaign=DressRehearsalNov2023 and core.runs=23387"

export MQL_REQUEST_FDPROD23_HD_NU="files from higuera:fardet-hd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_nu_dune10kt_1x2x6.fcl__v09_78_01d01__preliminary limit 1"
export MQL_REQUEST_FDPROD23_HD_ANU="files from higuera:fardet-hd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_anu_dune10kt_1x2x6.fcl__v09_78_01d01__preliminary limit 1"

export MQL_REQUEST_FDPROD23_VD_NU="files from higuera:fardet-vd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_nu_dunevd10kt_1x8x6_3view_30deg.fcl__v09_75_03d00__preliminary limit 1"
export MQL_REQUEST_FDPROD23_VD_ANU="files from higuera:fardet-vd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_anu_dunevd10kt_1x8x6_3view_30deg.fcl__v09_75_03d00__preliminary limit 1"
