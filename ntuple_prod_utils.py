import subprocess
import sys
import os
from argparse import ArgumentParser as ap

def check_n(args):
  if args.n < 1:
    print('Error: need to supply -n greater than 0')
    exit(1) 

def get_nfiles_justin(args):
  
  ## TODO -- check args.dids, pfns

  check_n(args)

  dids = []
  pfns = []
  for i in range(args.n):

    ## Run justin-get-file
    proc = subprocess.run(
      [f'{os.environ["JUSTIN_PATH"]}/justin-get-file'],
      capture_output=True,
    )

    print('Return code', proc.returncode)
    print('Stdout', proc.stdout)
    print('Stderr', proc.stderr)
    ## If there's an error, then exit
    #if proc.returncode != 0:
    #  print('Error in justin-get-file within get_nfiles_justin.py')
    #  print(proc.stdout)
    #  print(proc.stderr)
    #  print(proc.stderr, file=sys.stderr)
    #  exit(proc.returncode)

    ## If there's not an error, but an empty string was returned,
    ## then we're out of files and should get out of the loop
    output = proc.stdout.decode('utf-8')
    if output == '': break

    #Split and add did and pfn to list
    dids.append(output.split()[0])
    pfns.append(output.split()[1])

  ## Print out pfns
  if len(pfns) == 0:
    #In case of no returns, just create empty files
    from pathlib import Path
    Path(args.dids).touch()
    Path(args.pfns).touch()
  else:
    print(' '.join(pfns))
    
    ##Also write out the DIDs to a file 
    with open(args.dids, 'w') as f:
      f.writelines([d + '\n' for d in dids])

    with open(args.pfns, 'w') as f:
      f.writelines([d + '\n' for d in pfns])

  exit(0)

def get_metadata(args):

  import json
  import ROOT as RT

  check = {
    '--version':args.version,
    '--fcl_name':args.fcl_name,
    '-o':args.o,
    '--root_file':args.root_file,
  }
  for flag,val in check.items():
    if val is None:
      raise ValueError(f'Need to supply {flag} with metadata command')
 
  from metacat.webapi import MetaCatClient
  mc = MetaCatClient()

  with open(args.dids, 'r') as f:
    lines = f.readlines()
  dids = [l.strip() for l in lines]

  parent_metadatas = []
  parents = []
  for n, did in enumerate(dids):

    parents.append({
      'namespace':did.split(':')[0],
      'name':did.split(':')[1],
    })

    if args.parent_metas is None:
      input_file = mc.get_file(did=did, with_provenance=False, with_metadata=True)
      parent_metadatas.append(input_file['metadata'])
    else:
      print('Getting json parent meta')
      with open(args.parent_metas[n], 'r') as fpm:
        parent_metadatas.append(json.load(fpm)['metadata'])
    #print('metadata', input_file['metadata'])

  to_copy = [
    'DUNE.campaign', 'MC.liquid_flow', 'MC.space_charge', 'MC.with_cosmics',
    'beam.momentum', 'beam.polarity', 'core.group', 'dune_mc.beam_energy',
    'dune_mc.detector_type', 'dune_mc.electron_lifetime', 'dune_mc.generators',
    'dune_mc.liquid_flow', 'dune_mc.space_charge', 'dune_mc.with_cosmics',
    'core.run_type', 'core.file_type', 'core.data_stream',
    'data_quality.online_good_run_list', 'detector.hv_value',
  ]

  ## TODO -- Confirm how these should be appended
  #to_concat = [
  #  'core.runs', 'core.runs_subruns'
  #]

  copied_metadatas = {tc:[] for tc in to_copy}
  #concated_metadatas = {tc:[] for tc in to_concat}
  runs_subruns = []
  #Loop over metadata sets from parents
  for md in parent_metadatas:

    #Loop over the fields to copy
    for tc in to_copy:

      #If the field is not in the parent dict, add None
      #This will be useful later when we check the size
      copied_metadatas[tc].append( 
        md[tc] if tc in md.keys()
        else None
      )

    ## These should be mandatory
    if 'core.runs' not in md.keys() or 'core.runs_subruns' not in md.keys():
      raise ValueError(f'Need core.runs and core.runs_subruns in all parents:'
                       f'\n{" ".join(dids)}')
    runs_subruns.append((md['core.runs'][0], md['core.runs_subruns'][0]))

  ##Check if we have any mixed metadata
  any_bad = False
  bads = []
  for tc, md_list in copied_metadatas.items():
    copied_metadatas[tc] = set(md_list)
    if len(copied_metadatas[tc]) > 1:
      any_bad = True
      bads.append([tc, list(copied_metadatas[tc])])
  if any_bad:
    message = 'Mixed metadata for fields\n'
    for bad in bads:
      #message += f'{tc}: {", ".join(md_list)}\n'
      #message += f'{bad[0]}: {", ".join(bad[1])}\n'
      print(bad)
    raise ValueError(message)

  ##Make them into a list of tuples, 
  output_md = {
    'core.application':'protoduneana.pdspana',
    'core.application.family':'protoduneana',
    'core.application.name':'pdspana',
    'core.application.version':args.version,
    'core.data_tier':'root-tuple-virtual',
    'core.file_format':'root',
    'dune.fcl_version_tag':args.version,
    'dune.fcl_name':args.fcl_name,
  }

  ##Format the runs/subruns info
  output_md['core.runs'] = []
  output_md['core.runs_subruns'] = []
  for r, sr in set(runs_subruns):
    output_md['core.runs'].append(r)
    output_md['core.runs_subruns'].append(sr)

  ##Put in the copied md
  for key, val in copied_metadatas.items():
    if next(iter(val)) is None: continue
    output_md[key] = next(iter(val))

  ##Get Nevents from root file
  ##Might need to make configurable i.e. tree name
  fIn = RT.TFile.Open(args.root_file)
  tree = fIn.Get(args.tree_name)
  nevents = tree.GetEntries()
  output_md['core.event_count'] = nevents
  

  ##Extract memory and time info from log
  if args.log_file is not None:
    with open(args.log_file, 'w') as flog:
      lines = [
        l.strip('\n')
        for l in flog.readlines()
        if (
          ('TimeReport' in l and 'CPU' in l) or
          ('MemReport' in l and 'Vm' in l)
        )
      ]

    for l in lines:
      if 'TimeReport' in l:
        formatted = l.strip('TimeReport ').replace('=','').split()
        output_md['info.cpusec'] = float(formatted[formatted.index('CPU')+1])
        output_md['info.wallsec'] = float(formatted[formatted.index('Real')+1])

      elif 'MemReport' in l:
        formatted = l.strip('MemReport ').replace('=','').split()
        output_md['info.memory'] = float(formatted[formatted.index('VmHWM')+1])


  output = {
    'parents':parents,
    'metadata':output_md,
  }
  # Serializing json
  json_object = json.dumps(output, indent=2)
   
  # Writing to sample.json
  with open(args.o, "w") as outfile:
    outfile.write(json_object)


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('command', type=str, help='Which command',
                      choices=[
                        'get_nfiles_justin', 'metadata',
                      ])
  parser.add_argument(
    '-n', type=int,
    help='Nfiles -- use with get_nfiles_justin',
    default=1)

  parser.add_argument('--dids', type=str, help='file containing dids')
  parser.add_argument('--pfns', type=str, help='file containing pfns')

  parser.add_argument('-o', type=str, help='Output file',
                      default=None)
  parser.add_argument('--version', type=str, help='Version for metadata',
                      default=None)
  parser.add_argument('--fcl_name', type=str, help='Fcl name',
                      default=None)
  parser.add_argument('--root_file', type=str, help='Root file for metadata',
                      default=None)
  parser.add_argument('--log_file', type=str, help='Log file for metadata',
                      default=None)
  parser.add_argument('--parent_metas', type=str, nargs='+')
  parser.add_argument('--tree_name', type=str, default='pduneana/beamana')
  args = parser.parse_args()

  commands = {
    'get_nfiles_justin':get_nfiles_justin,
    'metadata':get_metadata,
  }

  commands[args.command](args)
