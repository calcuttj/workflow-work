# Generic Script
`generic.jobscript` can run `lar` using various fcl files and on various samples (data or MC from PDHD or PDSP).
### Test command
<pre>justin-test-jobscript --mql &lt;MQL_REQUEST&gt; --jobscript generic.jobscript
</pre>

### Optional environment variables
`--env NEVENTS` -- controls how many events to parse (default -1/all)

`--env NFILES` -- controls how many files to go through (default 1)

`--env FCLFILE` -- controls which fcl file to use (default `pduneana_Prod4a_MC_sce.fcl`)

`--env OUTPREFIX` -- controls prefix to output file (default `duneana_ntuple`) 

`--env DUNE_VERSION` -- controls which version of dunesw to use (default `v09_79_00d00`)

`--env DUNE_QUALIFIER` -- controls which qualifiers of dunesw to use (default `e26:prof`)




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
`--env NTUPLE=1` -- controls which output flag (NTUPLE: `-T` or default: `-o`) is used.

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
`--env HDF5JOB=1` -- Tells the job to run in a subshell with LD_PRELOAD set so hdf5 can be streamed via xrootd
