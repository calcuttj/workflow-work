from argparse import ArgumentParser as ap

def get_env_dict(f):
  return {l.split('=')[0].strip():'='.join(l.split('=')[1:]).strip()  for l in f.readlines() if len(l.split('=')) > 1}

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-f', required=True, help='fife_launch env file')
  parser.add_argument('-j', required=True, help='justin env file')
  args = parser.parse_args()

  with open(args.f, 'r') as f:
    f_lines = get_env_dict(f) #{l.split('=')[0]:'='.join(l.split('=')[1:])  for l in f.readlines() if len(l.split('=')) > 1}
  with open(args.j, 'r') as f:
    j_lines = get_env_dict(f)

  only_fife = []
  only_justin = []
  diff=[]

  only_fife = [
    k + '=' + v + '\n' for k, v in f_lines.items() if k not in j_lines.keys()
  ]

  only_justin = [
    k + '=' + v + '\n' for k, v in j_lines.items() if k not in f_lines.keys()
  ]

  both = [
    k for k in f_lines.keys() if k in j_lines.keys()
  ]

  print('Only in fife:', len(only_fife))
  with open('only_fife.txt', 'w') as f: f.writelines(only_fife)
  print('Only in justin:', len(only_justin))
  with open('only_justin.txt', 'w') as f: f.writelines(only_justin)

  diff = [
    f'{k}\n\tJustin: {j_lines[k]}\n\tfife: {f_lines[k]}\n' for k in both if j_lines[k] != f_lines[k]
  ]
  with open('diff_env.txt', 'w') as f: f.writelines(diff)
  print('Different:', len(diff))
