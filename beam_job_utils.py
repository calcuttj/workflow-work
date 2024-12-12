from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
import json
mc = MetaCatClient()

def check_args_md(args):
  for arg in [args.nevents, args.run, args.subrun]:
    if args is None:
      print('Need to provide nevents, run, and subrun for metadata')
      exit(1)

def get_run_subrun(args):
  md = mc.get_file(did=args.i, with_metadata=True)
  print(md['metadata']['core.runs'][0],
        md['metadata']['core.runs_subruns'][0])

def make_metadata(args):

  check_args_md(args)

  if args.json is not None:
    if args.o == args.json:
      raise ValueError("Cannot use same name as input json")
    with open(args.json, 'r') as f:
      in_dict = json.load(f)
  else:
    in_dict = {
      'metadata': {}
    }
  if args.exclude is not None:
    for exc in args.exclude:
      if exc in in_dict['metadata'].keys():
        del in_dict['metadata'][exc]

  if args.overrides is not None:
    for override in args.overrides:
      split = override.split('=')
      #print(split[0], split[1])
      val = split[1]
      if 'time' in split[0].lower():
        val = float(val)
      in_dict['metadata'][split[0]] = val #split[1]

  #print(in_dict)

  if args.parent is not None:
    name = args.parent.split(':')[1]
    namespace = args.parent.split(':')[0]
    in_dict['parents'] = [
      {"name": name,
       "namespace": namespace 
      }
    ]

  in_dict['metadata']['core.event_count'] = args.nevents
  in_dict['metadata']['core.runs'] = [args.run]
  in_dict['metadata']['core.runs_subruns'] = [args.subrun]
  in_dict['metadata']['core.first_event_number'] = args.event
  in_dict['metadata']['core.last_event_number'] = (
      args.event + args.nevents - 1)

  if args.past_fcls is not None:
    if ((args.past_apps is None) or (len(args.past_fcls) != len(args.past_apps))
        #(args.past_vers is None) or (len(args.past_fcls) != len(args.past_vers))
    ):
      raise ValueError('Need to provide same number of past apps, versions, and fcls')

    in_dict['metadata']['origin.applications.names'] = args.past_apps
    in_dict['metadata']['origin.applications.config_files'] = {
      args.past_apps[i]:args.past_fcls[i] for i in range(len(args.past_apps))
    }

    # in_dict['metadata']['origin.applications.versions'] = {
    #   args.past_apps[i]:args.past_vers[i]
    #   for i in range(len(args.past_apps))
    # }

    in_dict['metadata']['origin.applications.versions'] = {
      args.past_apps[i]:in_dict['metadata']['core.application.version']
      for i in range(len(args.past_apps))
    }
    
  # if args.past_fcls is not None:
  #   if args.past_apps is None or len(args.past_fcls) != len(args.past_apps):
  #     raise ValueError('Need to provide same number of past apps and fcls')

  #   in_dict['metadata']['origin.applications.names'] = args.past_apps
  #   in_dict['metadata']['origin.applications.config_files'] = {
  #     args.past_apps[i]:args.past_fcls[i] for i in range(len(args.past_apps))
  #   }

  #   in_dict['metadata']['origin.applications.versions'] = {
  #     args.past_apps[i]:in_dict['metadata']['core.application.version']
  #     for i in range(len(args.past_apps))
  #   }

  # Serializing json
  json_object = json.dumps(in_dict, indent=2)
   
  # Writing to sample.json
  with open(args.o, "w") as outfile:
    outfile.write(json_object)


if __name__ == '__main__':
  parser = ap()
  parser.add_argument(
    'routine',
    type=str,
    choices=['get_run_subrun', 'make_metadata'],
  )

  parser.add_argument('-i', type=str, default=None)
  parser.add_argument('-o', type=str, default=None)
  parser.add_argument('--json', type=str, default=None)
  parser.add_argument('--run', type=int, default=None,)
  parser.add_argument('--subrun', type=int, default=None,)
  parser.add_argument('--exclude', type=str, default=None, nargs='+')
  parser.add_argument('--overrides', type=str, default=None, nargs='+')
  parser.add_argument('--parent', type=str, default=None)
  parser.add_argument('--event', type=int, default=0)
  parser.add_argument('--nevents', type=int, default=None)
  parser.add_argument('--past_fcls', type=str, nargs='+')
  parser.add_argument('--past_apps', type=str, nargs='+')
  parser.add_argument('--past_vers', type=str, nargs='+')

  args = parser.parse_args()

  routines = {
    'get_run_subrun':get_run_subrun,
    'make_metadata':make_metadata,
  }

  routines[args.routine](args)