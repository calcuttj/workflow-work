from argparse import ArgumentParser as ap
from rucio.client import Client as RucioClient


def good_line(l):
  if (
      '---' in l or
      'Total files' in l or
      'Total size' in l or
      'SCOPE:NAME' in l
  ): return False
  return True

def process_file(filename, n1=5):
  with open(filename, 'r') as f:
    lines = [l.split('_') for l in f.readlines() if good_line(l)]

  lines = ['_'.join(l[n1:n1+2]) for l in lines]
  #print('N Files:', len(lines))

  setlines = set(lines)
  for i in setlines:
    n = lines.count(i)
    if n > 1: print(i, n)
  return setlines

  #counts = {
  #  i:lines.count(i) for i in setlines
  #}


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-i', type=str, nargs='+')
  parser.add_argument('--type', type=str, default='hd')
  args = parser.parse_args()

  rc = RucioClient()

  print('Duplicates:')
  files = dict()
  n1=(5 if args.type == 'vd' else 3)
  for i in args.i:
    print(i)
    #files[i] = process_file(i, )
    full_rucio_files = [f['name'] for f in rc.list_files(*(i.split(':')))]
    rucio_files = dict()
    for f in full_rucio_files:
      if '_'.join(f.split('_')[n1:n1+2]) not in rucio_files.keys(): rucio_files['_'.join(f.split('_')[n1:n1+2])] = []
      rucio_files['_'.join(f.split('_')[n1:n1+2])].append(f)
    rucio_ids = ['_'.join(f.split('_')[n1:n1+2]) for f in full_rucio_files]
    setlines = set(rucio_ids)
    bad_files = []
    for j in setlines:
      n = rucio_ids.count(j)
      if n > 1:
        print(j, n)
        print(j, rucio_files[j])
        for bf in rucio_files[j]: bad_files.append(bf + '\n')
    files[i] = setlines
    print()
    with open(f'bad_{i}.txt'.replace(':', '_'), 'w') as badfiles_file:
      badfiles_file.writelines(bad_files)

  for i, ifiles in files.items():
    print('Checking', i)
    for j, jfiles in files.items():
      if j == i: continue
      for f in ifiles:
        if f not in jfiles: print(f'{f} from {i} not in {j}') 
    print()

