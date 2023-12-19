from argparse import ArgumentParser as ap
import json


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-i', type=str)
  args = parser.parse_args()

  with open(args.i) as f:
    imported_json = json.load(f)

  print(imported_json)
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
  }

  to_del = ['file_name', 'file_size', 'parents']

  for f, newf in fields.items():
    if f not in imported_json.keys(): continue
    imported_json[newf] = imported_json[f]
    del imported_json[f]

  for td in to_del: del imported_json[td]

  runs = imported_json['runs']
  runs_runs = [r[0] for r in runs]
  runs_subruns = [r[1] for r in runs]
  imported_json['core.runs'] = runs_runs
  imported_json['core.runs_subruns'] = runs_subruns
  del imported_json['runs']

  print(imported_json)

  # Serializing json
  json_object = json.dumps(imported_json, indent=2)
   
  # Writing to sample.json
  with open(args.i, "w") as outfile:
      outfile.write(json_object)

