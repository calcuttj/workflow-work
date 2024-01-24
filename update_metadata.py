from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-q', type=str, required=True, help='Metacat query')
  parser.add_argument('--check', action='store_true')
  args = parser.parse_args()

  mc = MetaCatClient()
  mc_files = [(f['namespace'] + ':' + f['name'], f['metadata']) for f in mc.query(args.q, with_metadata=True)]
  for f,md in mc_files[:1]:
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

    mc.update_file(f, metadata=new_metadata)
