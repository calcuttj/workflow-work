from rucio.client import Client as RucioClient
from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

def get_one_wf(mc, w):
  files = mc.query(f"files where dune.workflow['workflow_id'] in ({w}) and "
                  'core.data_tier=root-tuple-virtual and '
                  'dune.output_status=confirmed ordered',
                  with_provenance=True,
                  with_metadata=False
                 )
  parents = mc.query(f"parents(files where dune.workflow['workflow_id'] in ({w}) and "
                  'core.data_tier=root-tuple-virtual and '
                  'dune.output_status=confirmed ordered) ordered',
                  with_provenance=True,
                  with_metadata=False
                 )
  return zip(files, parents)
   
def get_one_ana_only(mc, w):
  return mc.query(f"files where dune.workflow['workflow_id'] in ({w}) and "
                  'core.data_tier=root-tuple-virtual and '
                  'dune.output_status=confirmed',
                  with_provenance=True,
                  with_metadata=False
                 )

def add_parent(pname, f, all_parents):
  if pname not in all_parents.keys():
    all_parents[pname] = []
  name = f['name']
  namespace = f['namespace']
  all_parents[pname].append(f'{namespace}:{name}')

  

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-w', help='workflows', type=int, nargs='+')
  parser.add_argument('--ana_only', type=int, nargs='+')
  args = parser.parse_args()


  mc = MetaCatClient()

  all_parents = dict()
  if args.ana_only is not None:
    ana_only_files = [get_one_ana_only(mc, w) for w in args.ana_only]
    for wf in ana_only_files:
      for f in wf:
        #print(f)
        pname = f['parents'][0]['fid']
        #if pname not in all_parents.keys():
        #  all_parents[pname] = []
        #all_parents[pname].append(f) 
        add_parent(pname, f, all_parents)

  if args.w is not None:
    workflow_files = [get_one_wf(mc, w) for w in args.w]
    for wf in workflow_files:
      for f,p in wf:
        #p = f['parents'][0]['fid']
        #gp = mc.get_file(fid=p, with_metadata=False)['fid']
        #add_parent(gp, f, all_parents)
        pname = p['parents'][0]['fid']
        #if pname not in all_parents.keys():
        #  all_parents[pname] = []
        #all_parents[pname].append(f) 
        #print(pname, f)
        add_parent(pname, f, all_parents)

  #print(all_parents)
  nw = 0 if args.w is None else len(args.w)
  nana_only = 0 if args.ana_only is None else len(args.ana_only)
  good_parents = {p:fs for p, fs in all_parents.items() if len(fs) == (nw + nana_only)}
  print(len(good_parents))

  iall = 0
  if args.ana_only is not None:
    for i in range(len(args.ana_only)):
      print('Writing', args.ana_only[i])
      with open(f'{args.ana_only[i]}_to_merge.txt', 'w') as f:
        for fl in good_parents.values():
          f.write(f'{fl[iall]}\n')
      iall += 1

  if args.w is not None:
    for i in range(len(args.w)):
      print('Writing', args.w[i])
      with open(f'{args.w[i]}_to_merge.txt', 'w') as f:
        for fl in good_parents.values():
          f.write(f'{fl[iall]}\n')
      iall += 1
