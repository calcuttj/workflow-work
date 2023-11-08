# Generic Script
`generic.jobscript` can run `lar` using various fcl files and on various samples (data or MC from PDHD or PDSP).

## PDSP MC NTuples
### MQL
<pre>
workflow$ metacat query -s "files from dune:all where core.file_type=mc and core.data_tier=full-reconstructed and art.run_type=protodune-sp and dune.requestid=RITM1115963 and mc.space_charge=yes"
Files:        83648
Total size:   161124291092849 (161.124 TB)
</pre>

### FCL
`pduneana_Prod4a_MC_sce.fcl`

### Required options
`NTUPLE=1` -- controls which output flag (NTUPLE: `-T` or default: `-o`) is used.

## PDSP Data Keepup
### MQL
<pre>
workflow$ metacat query -s "files from dune:all where core.file_type=detector and core.data_tier=raw and core.run_type=protodune-sp"
Files:        922532
Total size:   4717153167566366 (4717.153 TB)
</pre>

### FCL
`protoDUNE_SP_keepup_decoder_reco.fcl`

## PDHP Data Keepup
### MQL
<pre>
workflow$ metacat query -s "files from dune:all where core.file_type=detector and core.data_tier=raw and 22949 in core.runs"
Files:        16
Total size:   66927584048 (66.928 GB)
</pre>

### FCL
`runpdhdwibethtpcdecoder.fcl`

### Required options
`HDF5JOB=1` -- Tells the job to run in a subshell with LD_PRELOAD set so hdf5 can be streamed via xrootd
