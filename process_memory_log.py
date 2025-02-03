from argparse import ArgumentParser as ap
# from rucio.client.replicaclient import ReplicaClient
import h5py as h5
import matplotlib.pyplot as plt

def get_from_rucio(query):
  from metacat.webapi import MetaCatClient
  mc = MetaCatClient()
  files = mc.query(query, with_metadata=False)
  dids = [{'scope':f['namespace'], 'name':f['name']} for f in files]
  
  rc = ReplicaClient()
  reps = rc.list_replicas(dids, schemes='pfns')
    
if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--query', '-q', type=str, default=None)
  parser.add_argument('--files', '-f', type=str, nargs='+')
  parser.add_argument('-o', type=str, default=None)
  #parser.add_argument('--vis', action='store_true')
  args = parser.parse_args()

  files = args.files

  memories = []

  for i, f in enumerate(files):
    print(f'File {i} {f}', end='\r')
    with open(f, 'r') as fin:
      lines = fin.readlines()

    for line in lines:
      if 'VmHWM =' in line:
        line = line.replace('=', '').split()
        memory = float(line[line.index('VmHWM')+1])
        print(memory)
        memories.append(memory)
        break

  if args.o is not None:
    with h5.File(args.o, 'w') as h5f:
      h5f.create_dataset('memory', data=memories) 
  
  # if args.vis:
  #  plt.plot(lines) 
  #  plt.show()
