from argparse import ArgumentParser as ap
import subprocess

def check_tar_str(args):
  if args.tars is None and not args.notars:
    raise RuntimeError('Must explicitly acknowledge that you do not want to provide a tar file to the job')

  if not args.notars:
    for tar in args.tars:
      if ':' not in tar:
        raise ValueError(
          'provided tars must be in the format '
          '[tarfile.tar]:[in-script environment variable]'
        )


def upload_tar(tar):
  results = subprocess.run(
    ['justin-cvmfs-upload', tar],
    capture_output=True,
  )
    
  if results.returncode != 0:
    print('justin-cvmfs-upload error:', results.stderr.decode('UTF-8'))
    raise RuntimeError(f'Could not upload tar file named "{tar}" to cvmfs')

  return results.stdout.decode('UTF-8').strip()

def build_tar_dict(args):

  tars = [i.split(':') for i in args.tars]
  return {tarstr[0]:(tarstr[1], upload_tar(tarstr[0])) for tarstr in tars}

def build_input(args):
  if is_hd(args):
    base = f'higuera:fardet-hd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_{args.flavor}_dune10kt_1x2x6.fcl__v09_78_01d01__preliminary'
  else: ##Defined to be VD
    base = f'higuera:fardet-vd__fd_mc_2023a__mc__hit-reconstructed__prodgenie_{args.flavor}_dunevd10kt_1x8x6_3view_30deg.fcl__v09_75_03d00__preliminary'

  return base + f' skip {args.skip}' + (f' limit {args.limit}' if args.limit is not None else '')

def build_output(args):
  #--output-pattern "*.pndr:fardet-vd-pandora_ritm1780305" --output-pattern "*Validation.root:fardet-vd-validation_ritm1780305"

  types = [('*reco2.root', 'reco2'), ('*reco2_ana.root', 'reco2ana')]
  if is_vd(args):
    types += [('*.pndr', 'pandora'), ('*Validation.root', 'validation')]

  outputs = [
    (f'{t[0]}:fardet-{args.det.lower()}-{t[1]}_ritm1780305_{args.flavor}_'
     f'{args.hc.lower()}_skip{args.skip}' +
     (f'_limit{args.limit}' if args.limit is not None else '_end') +
     ('_test' if args.test else '')
    )
    for t in types
  ]
  results = []
  for o in outputs:
    results.append('--output-pattern')
    results.append(o)
  return results 

def is_hd(args):
  return args.det.upper() == 'HD'

def is_vd(args):
  return args.det.upper() == 'VD'

def check_flavor(args):
  hd_flavors = ['nu', 'anu', 'nue', 'anue', 'nutau', 'anutau']
  vd_flavors = [
    'nu', 'anu',
    'nu_numu2nue_nue2nutau', 'anu_numu2nue_nue2nutau',
    'nu_numu2nutau_nue2numu', 'anu_numu2nutau_nue2numu',
  ]
  if is_hd(args) and args.flavor not in hd_flavors:
    raise ValueError(f'Must choose flavor in {hd_flavors} when running HD')
  elif (is_vd(args) and args.flavor not in vd_flavors):
    raise ValueError(f'Must choose flavor in {vd_flavors} when running VD')

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--script', type=str, default='dec2023_FD_MC_production.jobscript')
  parser.add_argument('--det', type=str, choices=['HD', 'VD', 'hd', 'vd'], default='HD')
  parser.add_argument('--hc', type=str, choices=['FHC', 'fhc', 'RHC', 'rhc'], default='FHC')
  parser.add_argument('--flavor', type=str,
                      choices=['nu', 'anu', 'nue', 'anue', 'nutau', 'anutau',
                               'nu_numu2nue_nue2nutau', 'anu_numu2nue_nue2nutau',
                               'nu_numu2nutau_nue2numu', 'anu_numu2nutau_nue2numu',
                              ],
                      default='nu')
  parser.add_argument('--test', action='store_true', help='Switch the scope to "usertests"')
  parser.add_argument('--no_out', action='store_true', help='Prevent output from being saved')
  parser.add_argument('--dset_postfix', type=str, default='', help='Postfix to give to the output pattern')
  parser.add_argument('--dry_run', action='store_true')
  parser.add_argument('--tars', nargs='+', type=str)
  parser.add_argument('--notars', action='store_true', help='Must explicitly say there are no tars to be provided')
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--limit', type=int, default=None)
  parser.add_argument('--lifetime', default=None, type=int, help='Requested lifetime in seconds')
  parser.add_argument('--nevents', type=int, default=None)
  args = parser.parse_args()

  ##Check that the tar options are valid
  check_tar_str(args)
  check_flavor(args)

  scope = 'usertests' if args.test else f'fardet-{args.det.lower()}'

  ##Start building up command
  cmd = [
    'justin', 'simple-workflow',
    '--jobscript', args.script,
    '--env', f'DETPROD={args.det.upper()}',
    '--env', f'HCPROD={args.hc.upper()}',
    '--scope', scope,
  ]

  ##Build up list of tars to upload and provide to job
  if args.tars is not None:
    tar_dict = build_tar_dict(args)
    for tarname, (scriptname, tardir) in tar_dict.items():
      cmd += ['--env', f'{scriptname}={tardir}']

  cmd += ['--mql', f'"files from {build_input(args)}"']
  cmd += build_output(args)

  if args.lifetime is not None:
    cmd += ['--wall-seconds', str(args.lifetime)]
  if args.nevents is not None:
    cmd += ['--env', f'NEVENTS={args.nevents}']


  print('Running')
  print(cmd[0], cmd[1])
  for i in range(2, len(cmd), 2):
    print('\t',cmd[i], cmd[i+1])

  if not args.dry_run:
    subprocess.run(' '.join(cmd), shell=True)
