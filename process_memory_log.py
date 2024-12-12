from argparse import ArgumentParser as ap
import matplotlib.pyplot as plt

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-i', type=str, required=True)
  parser.add_argument('--vis', action='store_true')
  args = parser.parse_args()

  with open(args.i, 'r') as f:
    lines = [int(l.split()[2]) for l in f.readlines() if 'Mem' in l]

  if args.vis:
    plt.plot(lines) 
    plt.show()
