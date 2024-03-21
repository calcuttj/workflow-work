from argparse import ArgumentParser as ap
from metacat.webapi import MetaCatClient
import numpy as np

def build_query(args):
  query = f'files where dune.workflow["workflow_id"] in ({args.w})'
  query += '' if args.extra is None else f' and {args.extra}'
  query += '' if args.limit is None else f' limit {args.limit}'
  return query
  
def get_seconds(i):
  return i['metadata']['dune.workflow']['jobscript_cpu_seconds']

def get_times(files):
  return np.array([get_seconds(i) for i in files])

def do_vis(times, args):
  if not args.vis: return

  import matplotlib.pyplot as plt

  plt.hist(times*1.e-3)
  plt.ylabel('N Jobs')
  plt.xlabel('Jobscript CPU Time [x1E3s]')
  plt.show()

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-w', type=int, required=True)
  parser.add_argument('--limit', type=int, default=None)
  parser.add_argument('--extra', type=str, default=None)
  parser.add_argument('-o', type=str, default=None)
  parser.add_argument('--vis', action='store_true')
  parser.add_argument('--fit', action='store_true')
  args = parser.parse_args()

  mc = MetaCatClient()
  query = build_query(args)

  print(query)

  files = mc.query(query, with_metadata=True)

  times = get_times(files)

  do_vis(times, args)

  print(f'Mean time: {np.mean(times):.0f}s')
  print(f'Max time: {np.max(times)}s')
  print(f'Mean + 2sigma: {np.mean(times) + 2*np.std(times):.0f}')
  print(f'Estimated 95%: {np.sort(times)[int(.95*len(times))]}s')
