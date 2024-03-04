import json
from argparse import ArgumentParser as ap


if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--json', type=str, default=None)
  parser.add_argument('--overrides', type=str, nargs='+') ##TODO -- Write help
  parser.add_argument('--parent', type=str, default=None)
  parser.add_argument('-o', type=str, required=True)
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

  print(in_dict)

  for override in args.overrides:
    split = override.split('=')
    print(split[0], split[1])
    in_dict['metadata'][split[0]] = split[1]

  print(in_dict)

  if args.parent is not None:
    in_dict['parents'] = [
      {"name": args.parent,
       "namespace": "protodune-hd"}
    ]


  # Serializing json
  json_object = json.dumps(in_dict, indent=2)
   
  # Writing to sample.json
  with open(args.o, "w") as outfile:
    outfile.write(json_object)

