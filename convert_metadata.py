from argparse import ArgumentParser as ap
import os
import json
#import root_metadata


def del_fields(the_json):
  to_del = ['file_name', 'parents', 'file_size']
  for td in to_del: del the_json[td]

  return the_json

def exchange_fields(the_json):
  fields = {
    'DUNE.campaign':'dune.campaign',
    'DUNE.requestid':'dune.requestid',
    'data_stream':'core.data_stream',
    'data_tier':'core.data_tier',
    'end_time':'core.end_time',
    'event_count':'core.event_count',
    'start_time':'core.start_time',
    'file_format':'core.file_format',
    'file_type':'core.file_type',
    'first_event':'core.first_event_number',
    'last_event':'core.last_event_number',
    'group':'core.group',
    'art.run_type':'core.run_type',
  }
  for f, newf in fields.items():
    if f not in the_json.keys(): continue
    the_json[newf] = the_json[f]
    del the_json[f]

  return the_json

#def get_application(the_json)

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-i', type=str)
  parser.add_argument('-c', type=str, default=None)
  parser.add_argument('-j', type=str, default=None)
  parser.add_argument('--det', type=str, default=None)
  parser.add_argument('--app_ver', type=str, default=None)
  parser.add_argument('--app', type=str, default=None)
  parser.add_argument('--parent', type=str, default=None)
  args = parser.parse_args()

  with open(args.i) as f:
    imported_json = json.load(f)

  print(imported_json)

  imported_json = exchange_fields(imported_json)

  imported_json = del_fields(imported_json)
  #to_del = ['file_name', 'parents', 'file_size']
  #for td in to_del: del imported_json[td]

  to_move = []
  over_meta = {
    tm[1]:imported_json[tm[0]] for tm in to_move
  }
  for tm in to_move: del imported_json[tm[0]]

  runs = imported_json['runs']
  runs_runs = [r[0] for r in runs]
  runs_subruns = [r[1] for r in runs]
  imported_json['core.runs'] = runs_runs
  imported_json['core.runs_subruns'] = [r*100000 + sr for r, sr in zip(runs_runs, runs_subruns)]
  del imported_json['runs']

  ##Check if various fields were reported. If so, add to metadata
  if args.c is not None:
    imported_json['dune.config_file'] = args.c

  if args.det is not None:
    imported_json['dune_mc.detector_type'] = args.det

  if args.app_ver is not None:
    imported_json['core.application.version'] = args.app_ver
  if args.app is not None:
    imported_json['core.application'] = args.app 
    imported_json['core.application.family'] = args.app.split('.')[0]
    imported_json['core.application.name'] = args.app.split('.')[1]

  if args.parent is not None:
    imported_json['core.parents'] = [{
      'file_name':args.parent,
    }]


  if args.j is not None:
    with open(args.j, 'r') as old_json_file:
      old_json = json.load(old_json_file)
      imported_json['dune_mc.gen_fcl_filename'] = old_json['metadata']['dune_mc.gen_fcl_filename']
      imported_json['core.run_type'] = old_json['metadata']['core.run_type']

  print(imported_json)
  over_meta['metadata'] = imported_json
  #over_meta['size'] = os.path.getsize(args.i.replace('.json', ''))

  # Serializing json
  #json_object = json.dumps(imported_json, indent=2)
  json_object = json.dumps(over_meta, indent=2)
   
  # Writing to sample.json
  with open(args.i, "w") as outfile:
      outfile.write(json_object)

