from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
from rucio.client import Client as RucioClient

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-q', type=str, required=True, help='Metacat query used as input to request')
  parser.add_argument('-d', type=str, required=True, help='Rucio dataset used as output of request')
  parser.add_argument('--type', type=str, default='reco2')
  parser.add_argument('-o', type=str, default='missing_files.txt')
  args = parser.parse_args()

  mc = MetaCatClient()
  mc_files = [f['name'] for f in mc.query(args.q)]

  print(mc_files[0])

  rc = RucioClient()
  last_index = -3 if args.type == 'reco2' else -4
  rucio_files = ['_'.join(f['name'].split('_')[:last_index]) + '.root' for f in rc.list_files(*(args.d.split(':')))]

  print(rucio_files[0])

  bad_files = [i + '\n' for i in mc_files if i not in rucio_files]
  print(bad_files)
  print(len(bad_files))

  with open(args.o, 'w') as f:
    f.writelines(bad_files)
