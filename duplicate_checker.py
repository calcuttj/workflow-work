from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--workflows', type=str, nargs='+')
  parser.add_argument('--tier', type=str, default=None)
  parser.add_argument('--list', action='store_true')
  args = parser.parse_args()
  mc_client = MetaCatClient()

  query = ("files where dune.workflow['workflow_id'] in "
           f"({','.join(args.workflows)})")
  if args.tier is not None:
    query += f" and core.data_tier='{args.tier}'"
  

  files = [i for i in mc_client.query(query)]
  print("Total queried files:", len(files))

  ids = ['_'.join(f['name'].split('_')[3:6]) for f in files]
  counts = {i:ids.count(i) for i in ids}
  dupes = [i for i,c in counts.items() if c > 1]
  print("N duplicates:", len(dupes))

  if args.list:
    for d in set(dupes):
      print(d)
