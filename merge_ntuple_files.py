from rucio.client import Client as RucioClient
rc = RucioClient()
import json
import os
from math import ceil
import subprocess
import yaml
from argparse import ArgumentParser as ap
from metacat.webapi import MetaCatClient
mc = MetaCatClient()

def distribute(nlines, nsplits):
    base, extra = divmod(nlines, nsplits)
    return [base + (i < extra) for i in range(nsplits)]

def get_pfn(rep):
  rses = rep['rses']
  #print(rses)
  if len(rses) == 0:
    return None
  elif 'DUNE_US_FNAL_DISK_STAGE' in rses.keys():
    return rses['DUNE_US_FNAL_DISK_STAGE'][0]
  else:
    return list(rses.values())[0][0]

def get_replicas(did_lists):
  results = []

  for dl in did_lists:
    #print(dl)
    reps = rc.list_replicas(dl)
    pfns = [get_pfn(r) for r in reps]
    pfns = [p for p in pfns if p is not None]
    results += pfns
    #break
  return results

def add_file_unique(unique_fields, results, f, fname=''):
  for uf in unique_fields:
    if uf not in f:
      raise ValueError(uf, 'not found in file', f)
    if uf not in results:
      results[uf] = f[uf]
    else:
      if results[uf] != f[uf]:
        raise ValueError('Tried to merge 2 different metadata for', uf, 'First',
                         results[uf], 'Second', f[uf], fname)

def merge_file(to_merge, results, f):
  for tm in to_merge:
    if tm not in f:
      raise ValueError(tm, 'not found in file', f)
    if tm not in results:
      results[tm] = f[tm]
    else:
      results[tm] += f[tm]

def get_metadata(args, mc_did_lists, outname):

  unique_fields, to_merge, as_set = get_config(args)

  total_event_count = 0
  
  results = {
    'parents':[],
    'name':outname,
    'namespace':args.namespace,
    'metadata':{'core.data_tier':'root-tuple'},
  }
  
  for mc_did_list in mc_did_lists:
    files = mc.get_files(mc_did_list)
    for i, f in enumerate(files):
      if not i % 1000: print(f'{i}/{len(files)}', end='\r')
      add_file_unique(unique_fields, results['metadata'], f['metadata'], f['name'])
      merge_file(to_merge, results['metadata'], f['metadata'])
      # total_event_count += f['metadata']['core.event_count']
      results['parents'] += f['parents']
  

  return finish_metadata(args, as_set, outname, results)

def split_metadata(args):

  the_lists = get_lists(args)
  mc_did_lists = the_lists.mc_did_list
  print('Getting metadata')

  unique_fields, to_merge, as_set = get_config(args)

  total_event_count = 0
  
  end = args.start + args.n if args.n > 0 else len(mc_did_lists) + 1
  for j, mc_did_list in enumerate(mc_did_lists):
    if j < args.start: continue
    if j >= end: break
    results = {
      'parents':[],
      'name':f'{args.o}{j}.root',
      'namespace':args.namespace,
      'metadata':{
        'core.data_tier':'root-tuple',
        'dune.dataset_name':f'{args.namespace}:{args.namespace}_{args.dataset}',
      },
    }
  
    files = mc.get_files(mc_did_list)
    for i, f in enumerate(files):
      if not i % 1000: print(f'{i}/{len(files)}', end='\r')
      add_file_unique(unique_fields, results['metadata'], f['metadata'], f['name'])
      merge_file(to_merge, results['metadata'], f['metadata'])
      # total_event_count += f['metadata']['core.event_count']
      results['parents'] += f['parents']

    results = finish_metadata(args, as_set, f'{args.o}{j}.root', results)
    json_object = json.dumps(results, indent=2)
    with open(f'{args.o}{j}.root.json', 'w') as fjson:
      fjson.write(json_object)

def get_config(args):
    unique_fields = [
    # 'beam.polarity',
    # 'core.application',
    'core.application.family',
    'core.application.name',
    'core.application.version',
    'core.data_stream',
    'core.file_format',
    'core.file_type',
    # 'core.group',
    'core.run_type',
    # 'dune.fcl_name',
    # 'dune.fcl_version_tag',
    #'mc.liquid_flow',
    #'mc.space_charge',
    #'mc.with_cosmics',
  ]
    to_merge = [
    'core.runs',
    'core.runs_subruns',
  ]
    as_set = [
    'core.runs',
    'core.runs_subruns',
  ]

    if args.type.lower() == 'mc':
      unique_fields += [
      'dune_mc.detector_type',
      'dune_mc.electron_lifetime',
      'dune_mc.generators',
      'dune_mc.liquid_flow',
      'dune_mc.space_charge',
      'dune_mc.with_cosmics',
    ]
  
    if args.yaml is not None:
      with open(args.yaml, 'r') as fin: 
        config = yaml.safe_load(fin)
      unique_fields = config['unique_fields']
      to_merge = config['to_merge']
      as_set = config['as_set']
    return unique_fields,to_merge,as_set

def finish_metadata(args, as_set, outname, results):
    print('Finishing metadata')
    for field in as_set:
      results['metadata'][field] = list(set(results['metadata'][field]))

    results['size'] = os.path.getsize(outname)

    if not args.skip_checksum:
      proc = subprocess.run(['xrdadler32', outname], capture_output=True)
      if proc.returncode != 0:
        raise Exception('xrdadler32 failed', proc.returncode, proc.stderr)

      checksum = proc.stdout.decode('utf-8').split()[0]
      results['checksums'] = {'adler32':checksum}

    return results

def top_metadata(args):
  do_metadata(args, args.o)

def do_metadata(args, outname):
  the_lists = get_lists(args)
  mc_did_lists = the_lists.mc_did_list
  print('Getting metadata')
  
  topdict = get_metadata(args, mc_did_lists, outname)
  json_object = json.dumps(topdict, indent=2)
  ## TODO -- make this better -- better naming scheme and argument usage
  with open(f'{outname}.json', 'w') as fjson:
    fjson.write(json_object)

class Lists:
  def __init__(self, mc_did_list, did_list):
    self.mc_did_list = mc_did_list
    self.did_list = did_list

def from_name_list(args):
    with open(args.list, 'r') as f:
      did_list = [i.strip('\n') for i in f.readlines()]
      did_list = [{'scope':d.split(':')[0], 'name':d.split(':')[1]} for d in did_list]
      mc_did_list = [{'namespace':d['scope'], 'name':d['name']} for d in did_list]
      return Lists(mc_did_list, did_list)

def from_path_list(args):
    with open(args.list, 'r') as f:
      did_list = [d.split('/')[-1] for d in open_paths(args)]
      did_list = [{'scope':args.namespace, 'name':d} for d in did_list]
      mc_did_list = [{'namespace':d['scope'], 'name':d['name']} for d in did_list]
      return Lists(mc_did_list, did_list)

def from_dataset(args):
    scope = args.d.split(':')[0]
    name = args.d.split(':')[1]
    did_list = [{'name':d['name'], 'scope':d['scope']} for d in rc.list_files(name=name, scope=scope)]
    mc_did_list = [{'namespace':d['scope'], 'name':d['name']} for d in did_list]
    return Lists(mc_did_list, did_list)

def open_paths(args):
  with open(args.list, 'r') as f:
    return [i.strip('\n') for i in f.readlines()]

def do_merge(args):
    if not args.list_is_paths:
      the_lists = get_lists(args)
      pfns = get_replicas(the_lists.did_list)
    else:
      pfns = open_paths(args)
    print(pfns)
    print(len(set(pfns)), len(pfns))

    ##Now split for hadding
    # n = args.hadd_split
    # nits = ceil(len(pfns)/n)
    # pfns_lists = [pfns[i*n:(i+1)*n] for i in range(nits)]

    split_lens = distribute(len(pfns), args.hadd_split)
    pfns_lists = [pfns[sum(split_lens[:i]):sum(split_lens[:i+1])] for i in range(args.hadd_split)]
    
    print(len(pfns_lists), [len(pl) for pl in pfns_lists])


    temps = []
    end = args.start + args.n if args.n > 0 else len(pfns_lists) + 1
    for i, pfn_list in enumerate(pfns_lists):
      if i < args.start: continue
      if i >= end: break
      subprocess.run(['hadd', '-f', f'{args.o}{i}.root', *pfn_list])
      temps.append(f'{args.o}{i}.root')


    # subprocess.run(['hadd', '-f', args.o, *temps])
    #for t in temps:
    #  print('Removing temp file:', t)
    #  os.remove(t)

def get_lists(args):
    # n = args.hadd_split 
    if args.d is not None:
      the_lists = from_dataset(args)
    elif args.list is not None:
      if not args.list_is_paths:
        the_lists = from_name_list(args)
      else:
        the_lists = from_path_list(args)

    # did_lists = [the_lists.did_list[i*n:(i+1)*n] for i in range(ceil(len(the_lists.did_list)/n))]
    # mc_did_lists = [the_lists.mc_did_list[i*n:(i+1)*n] for i in range(ceil(len(the_lists.mc_did_list)/n))]
    
    
    split_lens = distribute(len(the_lists.did_list), args.hadd_split)
    did_lists = [the_lists.did_list[sum(split_lens[:i]):sum(split_lens[:i+1])] for i in range(args.hadd_split)]
    mc_did_lists = [the_lists.mc_did_list[sum(split_lens[:i]):sum(split_lens[:i+1])] for i in range(args.hadd_split)]
    
    
    print(len(did_lists))
    return Lists(mc_did_lists, did_lists)

def estimate_size(args):
  the_lists = get_lists(args)
  mc_did_lists = the_lists.mc_did_list
  print('Getting metadata')
  topdict = get_metadata(args, mc_did_lists)

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('routine', type=str, choices=['merge', 'metadata', 'split_metadata'], default='merge')
  parser.add_argument('--yaml', type=str, default=None)
  parser.add_argument('-d', type=str, help='dataset', default=None) ##TODO -- enable metacat query
  parser.add_argument('--list', type=str, help='Input file list', default=None)
  parser.add_argument('--type', type=str, help='data or mc', default='mc',
                      choices=['MC', 'mc', 'data', 'Data',])
  parser.add_argument('--list-is-paths', action='store_true', help='Whether input file list is paths')
  parser.add_argument('--skip-checksum', action='store_true', help='Whether to skip checksum getting if testing')
  # parser.add_argument('--list-limit', type=int,
  #                     help='Number of file replicas to list at once -- recommend O(1000) files per split',
  #                     default=1)
  parser.add_argument('--hadd-split', type=int,
                      help='Number of hadd iterations',
                      default=1)
  parser.add_argument('--start', type=int, default=0)
  parser.add_argument('-n', type=int, default=-1)
  parser.add_argument('-o', type=str, help='Output name')
  parser.add_argument('--namespace', type=str, required=True, help='Which namespace')
  parser.add_argument('--dataset', type=str, help='Dataset to inject to metadata')
  args = parser.parse_args()


  # mc_did_lists = get_lists(from_name_list, from_dataset, args)

  routines ={
    'metadata':top_metadata,
    'split_metadata':split_metadata,
    'merge':do_merge,
    'estimate_size':estimate_size,
  }

  routines[args.routine](args)
