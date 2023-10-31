# Generic Script
`generic_script.sh` can run `lar` on a fresh setup using various fcl files and on various samples (data or MC).

<pre>
  Usage: ./generic_script.sh -c FCL_FILE [optional flags] INPUT_FILE
  Optional flags:
    -n, --nevents NEVENTS   Controls number of events, default == -1 (all)
    -o OUTPUT_FILE          Output file name
    --ntuple                Required when creating ntuples
    --nosetup               Use when you don't want to setup dunesw (i.e. for testing)
    --samweb                Tells the script to get the xrootd URI from samweb
    --dry_run               Will spit out what will be run
</pre>

## PDSP MC NTuples
The script can be used to create ProtoDUNE-SP ntuples. Relevant samweb definition include `PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_0X` where `X` ranges [0, 9].

Get the `Nth` file (0-indexed) from the given samweb definition as follows:
<pre>
  samweb list-files defname:PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_00 with limit 1 with offset N
</pre>

From a fresh login, `lar` can be run with the `pduneana_Prod4a_MC_sce.fcl` fcl file for NEVENTS on a given file (with its full path) as:
<pre>
  ./generic_script.sh -c pduneana_Prod4a_MC_sce.fcl -n NEVENTS --ntuple -o OUTPUT_FILE --samweb FILE_FROM_SAM
</pre>

## PDSP Data Keepup
Get a raw PDSP data file (add `with offset N` to get the `Nth` file from the run)
<pre>
  samweb list-files 'run_number 5387 and run_type protodune-sp and data_tier raw' with limit 1
</pre>

Use `protoDUNE_SP_keepup_decoder_reco.fcl` to process the raw data:
<pre>
  ./generic_script.sh -c protoDUNE_SP_keepup_decoder_reco.fcl --samweb -n NEVENTS -o OUTPUT_FILE FILE_FROM_SAM
</pre>

## PDSP MC Reco
