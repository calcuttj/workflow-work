import submit_helper
from argparse import ArgumentParser as ap
import subprocess

MAX_STARTLINE=141839 ##0-indexed N lines in input file



def build_skip(args):
  if args.skip > 0:
    return ['--env', f'STARTLINE={args.skip}']
  else:
    return []

def check_skip_limit(args):
  if args.skip + args.limit > MAX_STARTLINE:
    raise ValueError('The sum of --skip and --limit '
                     f'cannot surpass {MAX_STARTLINE}')

def build_limit(args):
  return ['--monte-carlo', str(args.limit)]

def build_output(args):
  types = [('*reco.root', 'reco'), ('*.pndr', 'pandora')]
  sce_settings = ['E500', 'off']

  status = 'test' if args.test else 'official'

  outputs = [
    (f'*sce_{sce}{t[0]}:pdhd_1gev_beam_cosmics_{status}_sce_{sce.lower()}_{t[1]}' +
     f'_limit{args.limit}_skip{args.skip}' +
     ('_$JUSTIN_WORKFLOW_ID' if args.wid_out else '')
    )
    for t in types for sce in sce_settings
  ]
  results = []
  for o in outputs:
    results.append('--output-pattern')
    results.append(f"'{o}'")
  return results 

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--simpletar', action='store_true')
  parser.add_argument('--script', type=str, default='spring2024_PDHD_MC_production.jobscript')
  parser.add_argument('--test', action='store_true', help='Switch the scope to "usertests"')
  parser.add_argument('--wid_out', action='store_true', help='Add justin workflow id to end of output datasets')
  parser.add_argument('--no_out', action='store_true', help='Prevent output from being saved')
  parser.add_argument('--dry_run', action='store_true')
  parser.add_argument('--tars', nargs='+', type=str)
  parser.add_argument('--notars', action='store_true', help='Must explicitly say there are no tars to be provided')
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--limit', type=int, default=1)
  parser.add_argument('--job-lifetime', default=None, type=int, help='Requested job lifetime in seconds')
  parser.add_argument('--out-lifetime', default=None, type=int,
                      help='Lifetime to give to output datasets -- '
                           'applicable only if a new dataset is being created')
  parser.add_argument('--distance', default=30, type=str, help='Max site--rse distance')
  parser.add_argument('--memory', default=5500, type=str, help='Requested memory')
  parser.add_argument('--nevents', type=int, default=None)
  parser.add_argument('--slash', action='store_true')
  args = parser.parse_args()

  submit_helper.check_tar_str(args)
  check_skip_limit(args)

  scope = 'usertests' if args.test else 'hd-protodune'

  ##Start building up command
  cmd = [
    'justin', 'simple-workflow',
    '--jobscript', args.script,
    '--scope', scope,
    '--max-distance', str(args.distance),
    '--rss-mb', str(args.memory),
  ]

  if args.nevents is not None:
    cmd += ['--env', f'NEVENTS={args.nevents}']
  if args.job_lifetime is not None:
    cmd += ['--wall-seconds', str(args.job_lifetime)]
  if args.out_lifetime is not None:
    cmd += ['--lifetime-days', str(args.out_lifetime)]

  cmd += submit_helper.add_tar_cmd(args)

  cmd += build_skip(args)
  cmd += build_limit(args)
  cmd += build_output(args)

  print('Running')
  print(cmd[0], cmd[1])
  for i in range(2, len(cmd), 2):
    print('\t',cmd[i], cmd[i+1], '\\' if args.slash else '')

  if not args.dry_run:
    subprocess.run(' '.join(cmd), shell=True)
