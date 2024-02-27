from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
import multiprocessing as mp

def process_loop(args, mc, mc_files, iproc):
  nfiles = len(mc_files)
  for imcf, (f,md) in enumerate(mc_files):
    #print(f'{imcf}/{nfiles} Editing file', f)

    new_metadata = dict()

    subrun = md['core.runs_subruns'][0]
    run = md['core.runs'][0]
    #print(f, md['core.runs'], subrun)
    factor = 100000
    #print((subrun < run*factor))
    any_bad = False
    if (subrun < run*factor):
      any_bad = True
      new_subrun = subrun + run*factor
      print(iproc, f, 'subrun bad')
      new_metadata['core.runs_subruns'] = [new_subrun]

    if 'core.parents' not in md.keys():
      print(iproc, f, 'no parents')
      any_bad = True

    if md['core.application.name'] == 'anatree':
      parent = f.replace('_ana', '')
    else:
      parent = f[:f.find('__')] + '.root'

    new_metadata['core.run_type'] = md['dune_mc.detector_type']
    new_metadata['core.parents'] = {'file_name':parent},
    #print(new_metadata)
    #for k,v in new_metadata.items():
    #  print(k,v)

    if not args.check:
      mc.update_file(f, metadata=new_metadata)
      print('Updated')

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-q', type=str, required=True, help='Metacat query')
  parser.add_argument('--check', action='store_true')
  parser.add_argument('--procs', type=int, default=1, help='Number of threads')
  args = parser.parse_args()

  mc = MetaCatClient()
  mc_files = [(f['namespace'] + ':' + f['name'], f['metadata']) for f in mc.query(args.q, with_metadata=True)]

  print(len(mc_files), args.procs)
  nfiles = int(len(mc_files)/args.procs)
  print(nfiles)
  split_files = [mc_files[i*nfiles:(i+1)*nfiles] for i in range(args.procs)]
  procs = [mp.Process(target=process_loop, args=(args, mc, split_files[i], i)) for i in range(args.procs)]
  for p in procs: 
    p.start()
  for p in procs:
    p.join()

  #process_loop(mc, mc_files)  


  '''for f,md in mc_files[:1]:
    print('Editing file', f)

    new_metadata = dict()

    subrun = md['core.runs_subruns'][0]
    run = md['core.runs'][0]
    #print(f, md['core.runs'], subrun)
    factor = 100000
    print((subrun < run*factor))
    if (subrun < run*factor):
      new_subrun = subrun + run*factor
      new_metadata['core.runs_subruns'] = [new_subrun]

    if md['core.application.name'] == 'anatree':
      parent = f.replace('_ana', '')
    else:
      parent = f[:f.find('__')] + '.root'

    new_metadata['core.run_type'] = md['dune_mc.detector_type']
    new_metadata['core.parents'] = {'file_name':parent},
    #print(new_metadata)
    for k,v in new_metadata.items():
      print(k,v)

    mc.update_file(f, metadata=new_metadata)'''
