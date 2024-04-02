import sys

if __name__ == '__main__':
  with open(sys.argv[1], 'r') as f:
    lines = f.readlines()

  lines = [l.strip().split() for l in lines]

  newlines = []
  for line in lines:
    filenum = line[0]
    all_events = int(line[1])
    for i in range(int(all_events/10)+1):
      print(i, all_events, all_events - i*10)
      nevents = min([10, all_events - i*10])
      newlines.append(f'{filenum} {i} {nevents}\n')

  with open(sys.argv[2], 'w') as f:
    f.writelines(newlines)
