from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

def get_files_and_dupes(query):
  files = [i for i in mc_client.query(query)]

  # Just get ids
  ids = ['_'.join(f['name'].split('_')[3:6]) for f in files]

  # Count them
  counts = {i:ids.count(i) for i in ids}

  # Find the duplicates
  dupes = [i for i,c in counts.items() if c > 1]
  dupe_full_files = [
    f['name'] for f in files if '_'.join(f['name'].split('_')[3:6]) in dupes
  ]

  return files, dupes, dupe_full_files


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
  

  # Get the reco files
  files = [i for i in mc_client.query(reco_query)]
  files, dupes, dupe_full_files = get_files_and_dupes(reco_query)
  print("Total queried reco files:", len(files))
  print(f"{len(dupes)} inputs with duplicated reco output")
  print(f"{len(dupe_full_files)} duplicate reco outputs")

  print('\n---------------')

  ana_files, ana_dupes, ana_dupe_full_files = get_files_and_dupes(ana_query)
  print("Total queried ana files:", len(ana_files))
  print(f"{len(ana_dupes)} inputs with duplicated ana output")
  print(f"{len(ana_dupe_full_files)} duplicate ana outputs")

  # Look both ways -- see if an id is found in either duplicate list
  # for both the ana and reco files
  all_bad_files = [
    f['name'] for f in ana_files
     if '_'.join(f['name'].split('_')[3:6]) in ana_dupes
     or '_'.join(f['name'].split('_')[3:6]) in dupes
  ] + [
    f['name'] for f in files
     if '_'.join(f['name'].split('_')[3:6]) in ana_dupes
     or '_'.join(f['name'].split('_')[3:6]) in dupes
  ]



  print('\n---------------')
  print(f'{len(all_bad_files)} plagued output files')

  # Sort so it's easier to check output
  all_bad_files.sort(
    key = lambda a : '_'.join(a.split('_')[3:6])
  )

  if args.list:
    for d in all_bad_files:
      print(d)

  if args.save is not None:
    with open(args.save, 'w') as f:
      f.writelines([l+'\n' for l in all_bad_files])
