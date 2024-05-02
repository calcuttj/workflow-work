import check_pdhd_output
from argparse import ArgumentParser as ap

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-w', type=int, required=True)
  parser.add_argument('--unconfirmed', action='store_true')
  args = parser.parse_args()


  mc = check_pdhd_output.MetaCatClient()
  query = f'files where dune.workflow["workflow_id"] in ({args.w})'
  if not args.unconfirmed: query +=' and dune.output_status="confirmed"'

  files = [i for i in mc.query(query)]

  check_pdhd_output.check_rucio(args, files, 'pdhd_dr', index=-12)
