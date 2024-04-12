from metacat.webapi import MetaCatClient
from rucio.client.replicaclient import ReplicaClient
from argparse import ArgumentParser as ap
from check_pdhd_files import build_queries, get_pfns

class Args:
  def __init__(self, w):
    self.w = w

def check_pfn(pfn, missing_pfns):
  for mp in missing_pfns:
    #print('checking', missing_pfns)
    if pfn not in mp: return False
  return True

def get_file_num(f):
  return int(f.split('_')[-1].replace('.root', ''))

def check_rucio(args, files, name, index=-9):

  print('Checking', name)

  #Get rucio client API
  rc = ReplicaClient()
  no_rse_files = []
  no_rse_dicts = []

  #Have to separate the list of files into batches of 1k (can possible tune this)
  #so as to not overload the rucio server
  #(I think... it spits an error for O(10k))
  nfiles = len(files)
  for i_start in range(0, nfiles, 1000):

    #Build up list of did dictionaries
    dids = [
        {'scope':f['namespace'], 'name':f['name']}
        for f in files[i_start:i_start+1000]
    ]

    #Get the replicas and unpack here
    #reps = [f for f in rc.list_replicas(dids)]

    #Loop over the results from the replicas
    #for rep in reps:
    for rep in rc.list_replicas(dids):

      #If there are no RSEs add to list
      if len(rep['rses']) == 0:
        no_rse_files.append(f"{rep['scope']}:{rep['name']}\n")
        no_rse_dicts.append(rep)

      #I'm not sure if this is also necessary -- but it will spit out if any
      #replicas are unavailable
      for rse,state in rep['states'].items():
        if state != 'AVAILABLE':
          print(rep['name'], 'has state', state, 'at', rse)

  #Write out the no-replica files
  with open(f'{args.w}_{name}_missing_replicas.txt', 'w') as f:
    f.writelines(no_rse_files)
  
  #Also get the pfn/numbers from the files as well -- for other checks later
  #Might get rid of this since I think the order of ops should be
  # 1. Get the no-replica files
  # 2. Data management scrubs those from metacat and rucio DBs
  # 3. I recheck for makeup
  no_rse_pfns = get_pfns(no_rse_dicts, index)
  return no_rse_pfns

  


def check_input(args):
  mc = MetaCatClient()
  
  ##Get the files from the workflow
  queries = build_queries(args)
 
  mc = MetaCatClient()
  sce_off_pandora_files = mc.query(queries['sce_off_pandora'])
  sce_on_pandora_files = mc.query(queries['sce_on_pandora'])
  sce_off_reco_files = mc.query(queries['sce_off_reco'])
  sce_on_reco_files = mc.query(queries['sce_on_reco'])

##NEED TO CHECK ALL OF THESE

  file_events = [
      (get_file_num(f['metadata']['dune_mc.h4_input_file']),
       f['metadata']['core.first_event_number']-1)
      for f in sce_on_reco_files
  ]

  file_events = sorted(file_events, key=lambda x: (x[0], x[1]))
  #for fe in file_events:print(fe)

  with open(args.list, 'r') as f: lines = f.readlines()
  lines = [l.strip().split() for l in lines]
  lines = [(int(l[0]), int(l[1])*10) for l in lines]

  bad_files = []
  for l in lines:
    if args.verbose: print('Checking', l)
    if l in file_events:
      if args.verbose: print('Warning', l)
      bad_files.append(l)

  print('Bad files:', len(bad_files))
  for bf in bad_files: print(bf)

def process(args):
  queries = build_queries(args)
 
  mc = MetaCatClient()
  sce_off_pandora_files = [f for f in mc.query(queries['sce_off_pandora'], with_metadata=True)]
  sce_on_pandora_files = [f for f in mc.query(queries['sce_on_pandora'], with_metadata=True)]
  sce_off_reco_files = [f for f in mc.query(queries['sce_off_reco'], with_metadata=True)]
  sce_on_reco_files = [f for f in mc.query(queries['sce_on_reco'], with_metadata=True)]

  #Get PFN numbers for each subset of output
  pfns = {
    'sce_off_pandora_pfns':get_pfns(sce_off_pandora_files, -11),
    'sce_on_pandora_pfns':get_pfns(sce_on_pandora_files, -11),
    'sce_off_reco_pfns':get_pfns(sce_off_reco_files),
    'sce_on_reco_pfns':get_pfns(sce_on_reco_files),
  }

  # For each subset of output, see if the given pfn is NOT in the output
  missing_lists = []
  all_missing_pfns = []
  for name, pfn_list in pfns.items():
    print(name)
    missing = [i for i in range(1, args.n+1) if i not in pfn_list]
    missing_lists.append(missing)
    all_missing_pfns += missing
    print('\tMissing:', missing)

  all_missing_pfns = list(set(all_missing_pfns))
  all_missing_pfns.sort()
  print('All missing', all_missing_pfns)

  # Check that the pfn is missing from ALL sets of output
  real_missing_pfns = [pfn for pfn in all_missing_pfns if check_pfn(pfn, missing_lists)]
  #print(real_missing_pfns)

  with open(args.list, 'r') as f:
    lines = [[int(j) for j in i.strip().split()] for i in f.readlines()]

  to_makeup = []
  #print('Nonzero files')
  for pfn in real_missing_pfns:
    if lines[pfn+args.skip-1][-1] != 0:
      #print(pfn, lines[pfn+args.skip-1])
      to_makeup.append(' '.join([str(i) for i in lines[pfn+args.skip-1]]) + '\n')
  with open(args.w + '_makeup.txt', 'w') as f: f.writelines(to_makeup)


def replicas(args):
  #Get metacat client
  mc = MetaCatClient()

  #Build queries from workflow
  queries = build_queries(args)

  #Get the files
  sce_off_pandora_files = [f for f in mc.query(queries['sce_off_pandora'], with_metadata=True)]
  sce_on_pandora_files = [f for f in mc.query(queries['sce_on_pandora'], with_metadata=True)]
  sce_off_reco_files = [f for f in mc.query(queries['sce_off_reco'], with_metadata=True)]
  sce_on_reco_files = [f for f in mc.query(queries['sce_on_reco'], with_metadata=True)]

  #Check the files
  check_rucio(args, sce_off_pandora_files, 'sce_off_pandora', -11)
  check_rucio(args, sce_on_pandora_files, 'sce_on_pandora', -11)
  check_rucio(args, sce_off_reco_files, 'sce_off_reco')
  check_rucio(args, sce_on_reco_files, 'sce_on_reco')

if __name__ == '__main__':

  parser = ap()
  parser.add_argument('-w', type=str, default=None)
  parser.add_argument('-n', type=int, default=None)
  parser.add_argument('--list', type=str, default=None)
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--verbose', action='store_true')
  parser.add_argument('--routine', type=str, default='process',
                      choices=[
                        'process', 'check_input',
                        'replicas',
                      ])
  args = parser.parse_args()

  routines = {
    'process':process,
    'check_input':check_input,
    'replicas':replicas,
  }

  routines[args.routine](args)
