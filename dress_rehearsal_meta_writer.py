from argparse import ArgumentParser as ap
import json
from metacat.webapi import MetaCatClient


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--parent_did', type=str, default=None)
  parser.add_argument('-o', type=str, required=True)
  parser.add_argument('-n', type=int, default=1)
  parser.add_argument('-v', type=str, default='v09_85_00d00')
  parser.add_argument('--start', type=float, required=True)
  parser.add_argument('--end', type=float, required=True)
  parser.add_argument('--fcl', type=str, default='rerun_reco.fcl')

  args = parser.parse_args()

  to_inherit = [
    'beam.momentum',
    'beam.polarity',
    'core.first_event_number',
    'core.run_type',
    'core.runs',
    'core.subruns',
    'dune_mc.detector_type',
    'dune_mc.electron_lifetime',
    'dune_mc.gen_fcl_filename',
    'dune_mc.generators',
    'dune_mc.h4_input_file',
    'dune_mc.liquid_flow',
    'dune_mc.space_charge',
    'dune_mc.with_cosmics',
    'core.application',
    'core.application.family',
    'core.application.name',
  ]

  base = {
   'dune.campaign':'pdhd_dress_rehearsal',
   'core.start_time':args.start,
   'core.end_time':args.end,
   'dune.config_file':args.fcl,
  }

  parent_name = args.parent_did.split(':')[1]
  parent_namespace = args.parent_did.split(':')[0]

  mc = MetaCatClient()
  parent = mc.get_file(did=args.parent_did, with_metadata=True)
  print(parent)

  for field in to_inherit:
    print(field, parent['metadata'][field])
    base[field] = parent['metadata'][field]

  ##get event count
  event_count = (
    parent['metadata']['core.event_count'] if args.n < 0
    else args.n
  )
  last_event = (
    int(parent['metadata']['core.first_event_number']) + event_count - 1
  )
  base['core.last_event_number'] = last_event

  print(base)

  output =  {
    'metadata':base,
    'parents':[{'did':args.parent_did}],
  }

  # Serializing json
  json_object = json.dumps(output, indent=2)
   
  # Writing to sample.json
  with open(args.o, "w") as outfile:
      outfile.write(json_object)

