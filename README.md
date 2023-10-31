# Running NTuple Script
`ntuple_script.sh` uses ProtoDUNE-SP 1GeV Prod4a MC events. Relevant samweb definition include `PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_0X` where `X` ranges [0, 9].

From a fresh login, `lar` can be run with the `pduneana_Prod4a_MC_sce.fcl` fcl file for NEVENTS on a given file (with its full path) as:
<pre>
  ./ntuple_script.sh -c pduneana_Prod4a_MC_sce.fcl -n NEVENTS FILE_WITH_PATH
</pre>

If a user would like to run after DUNE software has been setup, specify the `--nosetup` flag.

## Using with samweb

The `Nth` file (0-indexed) can be accessed from a given samweb definition as follows:
<pre>
  samweb list-files defname:PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_00 with limit 1 with offset N
</pre>

Specifying the `--samweb` flag will tell the script to get the URI from samweb (using `samweb2xrootd`) so the user doesn't have to provide it themself:
<pre>
  ./ntuple_script.sh -c pduneana_Prod4a_MC_sce.fcl -n NEVENTS --samweb FILE_FROM_SAM
</pre>

