from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
from check_pdhd_files import build_queries, get_pfns

def check_pfn(pfn, missing_pfns):
  for mp in missing_pfns:
    #print('checking', missing_pfns)
    if pfn not in mp: return False
  return True

def get_file_num(f):
  return int(f.split('_')[-1].replace('.root', ''))

def check_input(args):
  mc = MetaCatClient()
  
  ##Get the files from the workflow
  queries = build_queries(args)
 
  mc = MetaCatClient()
  sce_off_pandora_files = mc.query(queries['sce_off_pandora'], with_metadata=True)
  sce_on_pandora_files = mc.query(queries['sce_on_pandora'], with_metadata=True)
  sce_off_reco_files = mc.query(queries['sce_off_reco'], with_metadata=True)
  sce_on_reco_files = mc.query(queries['sce_on_reco'], with_metadata=True)

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
  sce_off_pandora_files = mc.query(queries['sce_off_pandora'])
  sce_on_pandora_files = mc.query(queries['sce_on_pandora'])
  sce_off_reco_files = mc.query(queries['sce_off_reco'])
  sce_on_reco_files = mc.query(queries['sce_on_reco'])

  pfns = {
    'sce_off_pandora_pfns':get_pfns(sce_off_pandora_files, -11),
    'sce_on_pandora_pfns':get_pfns(sce_on_pandora_files, -11),
    'sce_off_reco_pfns':get_pfns(sce_off_reco_files),
    'sce_on_reco_pfns':get_pfns(sce_on_reco_files),
  }

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

  real_missing_pfns = [pfn for pfn in all_missing_pfns if check_pfn(pfn, missing_lists)]
  #print(real_missing_pfns)

  with open(args.list, 'r') as f:
    lines = [[int(j) for j in i.strip().split()] for i in f.readlines()]

  to_makeup = []
  print('Nonzero files')
  for pfn in real_missing_pfns:
    if lines[pfn+args.skip-1][-1] != 0:
      print(pfn, lines[pfn+args.skip-1])
      to_makeup.append(' '.join([str(i) for i in lines[pfn+args.skip-1]]) + '\n')
  with open(args.w + '_makeup.txt', 'w') as f: f.writelines(to_makeup)

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
                      ])
  args = parser.parse_args()

  routines = {
    'process':process,
    'check_input':check_input,
  }

  routines[args.routine](args)
