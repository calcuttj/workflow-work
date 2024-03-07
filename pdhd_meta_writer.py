import json
from argparse import ArgumentParser as ap


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--json', type=str, default=None)
  parser.add_argument('--overrides', type=str, nargs='+') ##TODO -- Write help
  parser.add_argument('--parent', type=str, default=None)
  parser.add_argument('-o', type=str, required=True)
  parser.add_argument('--run', type=int, default=None)
  parser.add_argument('--event', type=int, default=None)
  parser.add_argument('--nevents', type=int, required=True)
  args = parser.parse_args()

  if args.json is not None:
    if args.o == args.json:
      raise ValueError("Cannot use same name as input json")
    with open(args.json, 'r') as f:
      in_dict = json.load(f)
  else:
    in_dict = {
      'metadata': {}
    }

  #print(in_dict)

  if args.overrides is not None:
    for override in args.overrides:
      split = override.split('=')
      #print(split[0], split[1])
      in_dict['metadata'][split[0]] = split[1]

  #print(in_dict)

  if args.parent is not None:
    name = args.parent.split(':')[0]
    namespace = args.parent.split(':')[1]
    in_dict['parents'] = [
      {"name": name,
       "namespace": namespace 
      }
    ]

  in_dict['metadata']['core.event_count'] = args.nevents
  if args.run is not None:
    in_dict['metadata']['core.runs'] = [args.run]
    in_dict['metadata']['core.subruns'] = [args.run*100000 + 1]
  if args.event is not None:
    in_dict['metadata']['core.first_event_number'] = args.event
    in_dict['metadata']['core.last_event_number'] = (
        args.event + args.nevents - 1)



  # Serializing json
  json_object = json.dumps(in_dict, indent=2)
   
  # Writing to sample.json
  with open(args.o, "w") as outfile:
    outfile.write(json_object)

