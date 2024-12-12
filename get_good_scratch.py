from argparse import ArgumentParser as ap
import subprocess

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--jobs', '-j', type=str, required=True)
  parser.add_argument('--files', '-f', type=str, required=True)
  parser.add_argument('-o', type=str, required=True)
  parser.add_argument('--i1',type=int, default=-5)
  parser.add_argument('--i2',type=int, default=-4)
  args = parser.parse_args()

  with open(args.jobs, 'r') as f:
    job_lines = [l.split('@')[0].replace('.', '_') for l in f.readlines() if 'finished' in l]
  print(job_lines[0])

  with open(args.files, 'r') as f:
    files = {
      l.split('_')[args.i1] + '_' + l.split('_')[args.i2]:l
      for l in f.readlines()
    }


  good_files = [
    files[j] for j in job_lines if j in files.keys()
  ]

  print('Got', len(good_files), 'good files', len(set(good_files)))


  good_stat_files = []
  for l in good_files:
    hostport = 'root://fndca1.fnal.gov:1094'
    loc = l.strip().replace('/pnfs', '/pnfs/fnal.gov/usr')
    print('Checking', loc)
    res = subprocess.run(['xrdfs', hostport, 'stat', '-q', 'Offline', loc], capture_output=True)
    if res.returncode != 0:
      good_stat_files.append(l)
    else:
      print('Skipping', l.strip())

  with open(args.o, 'w') as fout:
    fout.writelines(good_stat_files)
