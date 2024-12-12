import submit_helper
from argparse import ArgumentParser as ap
import subprocess

def build_input(args):
  this_limit = ('' if args.limit is None else f'limit {args.limit}')
  #query = f'"files from usertests:calcuttj_PDSPProd4a_1GeV ordered skip {args.skip} {this_limit}"'
  query = f'"files from pdsp_mc_reco:PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_0{args.d} ordered skip {args.skip} {this_limit}"'

  results = ['--mql', query]
  return results 
def build_output(args):
  types = [('*pdspana.root', 'pdspana')]
  if not args.ana_only and not args.no_reco_out:
    #types.append(('*reco.root', 'reco'))
    types.insert(0, ('*reco.root', 'reco'))

  status = 'test' if args.test else 'official'

  if args.scratch:
    outputs = [f'{t[0]}:$FNALURL/calcuttj' for t in types]
  else:
    if args.end:
      limit_str = '_limit_end'
    elif args.limit is not None:
      limit_str = f'_limit{args.limit}'
    else:
      limit_str = ''
      
    outputs = [
      (f'{t[0]}:pdhd_1gev_rerun_{status}_sce_{args.sce.lower()}_{t[1]}' +
       limit_str +
       #('_limit_end' if args.end else f'_limit{args.limit}') +
       f'_skip{args.skip}' +
       ('_$JUSTIN_WORKFLOW_ID' if args.wid_out else '')
      )
      for t in types
    ]
  results = []
  for o in outputs:
    results.append('--output-pattern')
    results.append(f"'{o}'")
  return results 

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--simpletar', action='store_true')
  parser.add_argument('-d', type=int, help='Which PDSP dataset to run over',
                      default=0, choices=[i for i in range(10)])
  parser.add_argument('--script', type=str, default='vary_sce/run_varied_sce_prod.jobscript')
  parser.add_argument('--test', action='store_true', help='Switch the scope to "usertests"')
  parser.add_argument('--wid_out', action='store_true', help='Add justin workflow id to end of output datasets')
  parser.add_argument('--scratch', action='store_true', help='Write to scratch')
  parser.add_argument('--dry_run', action='store_true')
  parser.add_argument('--tars', nargs='+', type=str)
  parser.add_argument('--notars', action='store_true')
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--limit', type=int, default=None)
  parser.add_argument('--job-lifetime', default=None, type=int, help='Requested job lifetime in seconds')
  parser.add_argument('--out-lifetime', default=None, type=int,
                      help='Lifetime to give to output datasets -- '
                           'applicable only if a new dataset is being created')
  parser.add_argument('--distance', default=30, type=str, help='Max site--rse distance')
  parser.add_argument('--memory', default=5500, type=str, help='Requested memory')
  parser.add_argument('--nevents', type=int, default=None)
  parser.add_argument('--slash', action='store_true')
  parser.add_argument('--end', action='store_true')
  parser.add_argument('--ana_only', action='store_true')
  parser.add_argument('--no_reco_out', action='store_true')
  parser.add_argument('--sce', type=str,
                      choices=['nominal', 'plus', 'minus'],
                      default='nominal'
                     )
  args = parser.parse_args()

  submit_helper.check_tar_str(args)

  scope = 'usertests' if args.test else 'hd-protodune'

  ##Start building up command
  cmd = [
    'justin', 'simple-workflow',
    '--jobscript', args.script,
    '--scope', scope,
    '--max-distance', str(args.distance),
    '--rss-mb', str(args.memory),
    '--env', f'TYPE={args.sce}',
  ]

  if args.nevents is not None:
    cmd += ['--env', f'NEVENTS={args.nevents}']
  if args.job_lifetime is not None:
    cmd += ['--wall-seconds', str(args.job_lifetime)]
  if args.out_lifetime is not None:
    cmd += ['--lifetime-days', str(args.out_lifetime)]
  if args.ana_only:
    cmd += ['--env', 'ANAONLY=1']

  cmd += build_input(args)

  cmd += submit_helper.add_tar_cmd(args)

  cmd += build_output(args)

  print('Running')
  print(cmd[0], cmd[1])
  for i in range(2, len(cmd), 2):
    #print('\t',cmd[i], cmd[i+1], '\\' if args.slash else '')
    print('\t',' '.join(cmd[i:i+2]), '\\' if args.slash else '')

  if not args.dry_run:
    subprocess.run(' '.join(cmd), shell=True)
