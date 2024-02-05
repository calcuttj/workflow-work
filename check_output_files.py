from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
from rucio.client import Client as RucioClient

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--tiers', type=str, nargs='+',
                      help='Rucio dataset used as outputs of request')
  parser.add_argument('--type', type=str, default='hd')
  parser.add_argument('--postfix', type=str, default='')
  args = parser.parse_args()


  n1 = 3 if args.type == 'hd' else 5
  rc = RucioClient()
  files = dict()
  for tier in args.tiers:
    dataset = f'fardet-{args.type}:fardet-{args.type}-{tier}_ritm1780305_{args.postfix}'
    print(dataset)

    files[tier] = {
      '_'.join(
        f['name'].split('_')[n1:n1+2] + [f['name'].split('_')[n1+8]]
      ):f['name']
      for f in rc.list_files(*(dataset.split(':')))
    }
    print(len(files[tier]))
    #print(files[tier].values()[0])

    print()
  print()



  bad_files = []
  for k1,f1s in files.items():
    for k2, f2s in files.items():
      if k1 == k2: continue
      bad_files += [f1f+'\n' for f1i, f1f in f1s.items() if f1i not in f2s]

  #bad_files = [i + '\n' for i in stripped_mc_files if i not in stripped_rucio_files]
  #print(bad_files)
  bad_files = set(bad_files)
  for bf in bad_files: print(bf)
  print(len(bad_files))

  with open(f'fardet_{args.type}_disparate_files.txt', 'w') as f:
    f.writelines(bad_files)
