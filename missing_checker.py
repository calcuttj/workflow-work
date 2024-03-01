from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

def get_id(f):
  return '_'.join(f.split('_')[3:6])
  
def get_files(query):
  files = [i['name'] for i in mc_client.query(query)]
  ids = [get_id(f) for f in files]
  return files, ids
if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--workflows', type=str, nargs='+')
  parser.add_argument('--list', action='store_true')
  parser.add_argument('--save', type=str, default=None)
  args = parser.parse_args()
  mc_client = MetaCatClient()

  base_query = ("files where dune.workflow['workflow_id'] in "
                f"({','.join(args.workflows)})")
  reco_query = base_query + " and core.data_tier='full-reconstructed'"
  ana_query = base_query + " and core.data_tier='root-tuple-virtual'"
 
  # Get the files
  reco_files, reco_ids = get_files(reco_query)
  ana_files, ana_ids = get_files(ana_query)

  bad_reco_files = [
    f for f in reco_files if get_id(f) not in ana_ids 
  ]

  bad_ana_files = [
    f for f in ana_files if get_id(f) not in reco_ids 
  ]

  print(f'{len(bad_reco_files)} bad reco files')
  print(f'{len(bad_ana_files)} bad ana files')

  all_bad_files = bad_reco_files + bad_ana_files
  if args.list:
    for d in all_bad_files:
      print(d)

  if args.save is not None:
    with open(args.save, 'w') as f:
      f.writelines([l+'\n' for l in all_bad_files])
