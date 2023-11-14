import json
from argparse import ArgumentParser as ap
from metacat.webapi import MetaCatClient

base_fields = [
  'DUNE.campaign',
  'core.data_stream',
  'core.file_type',
  'core.run_type',
]

def make_client():
  client = MetaCatClient(server_url='https://metacat.fnal.gov:9443/dune_meta_prod/app',
                         auth_server_url='https://metacat.fnal.gov:8143/auth/dune',
                         timeout=30)

  return client

def get_metadata(client, did):
  print('Getting', did)
  return client.get_file(did=did)['metadata']

def get_exclusive(metadatas, fields=base_fields):
  #field_map = {} {f:[] for f in fields}
  field_map = {}
  for f in fields:
    field_map[f] = []
    for md in metadatas:
      field_map[f].append(md[f])

    field_map[f] = list(set(field_map[f]))
    if len(field_map[f]) != 1:
      raise Exception(f'Field {f} is not unique: {field_map[f]}')

    field_map[f] = field_map[f][0]
  return field_map

def get_event_info(metadatas):
  events = []
  runs = []
  subruns = []

  for md in metadatas:
    events += md['core.events']
    runs += md['core.runs']
    subruns += md['core.runs_subruns']

  return events, list(set(subruns)), list(set(runs))

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--files', nargs='+', type=str, required=True)
  parser.add_argument('-o', type=str, required=True)
  parser.add_argument('-v', action='store_true')
  parser.add_argument('--tier', required=True)
  args = parser.parse_args()


  client = make_client()
  metadatas = [get_metadata(client, f) for f in args.files]
  events, subruns, runs = get_event_info(metadatas)

  if args.v:
    print(events)
    print(subruns)
    print(runs)
  all_fields = get_exclusive(metadatas)

  output_name = args.o.strip('.json')
  the_format = output_name.split('.')[-1]

  print(all_fields)
  new_metadata = {
    'core.data_tier':args.tier,
    'core.events':events,
    'core.runs':runs,
    'core.subruns':subruns,
    'core.first_event_number':min(events),
    'core.last_event_number':max(events),
    'core.file_format':the_format,
    'core.event_count':len(events),
  }
  if output_name[-5:] != '.json': output_name += '.json'

  # Serializing json
  json_object = json.dumps(new_metadata, indent=2)
   
  # Writing to sample.json
  with open(output_name, "w") as outfile:
      outfile.write(json_object)

