from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap
import sys
import time

import json
import subprocess
import os
mc = MetaCatClient()

class FakeArgs:
  def __init__(self, args, outname, start_time, end_time, gen_fcl, end_fcl):
    self.overrides = ([] if args.overrides is None else args.overrides) + [
      f'dune.config_file={end_fcl}',      
      f'core.start_time={start_time}',
      f'core.end_time={end_time}',
    ]
    if gen_fcl is not None:
      self.overrides.append(f'dune_mc.gen_fcl_filename={gen_fcl}')
    self.json = args.json
    self.event = args.event
    self.nevents = args.nevents
    self.run = args.run
    self.subrun = args.subrun
    self.past_fcls = []
    self.past_apps = []
    self.past_vers = None
    self.o = outname
    self.exclude = args.exclude
    self.parent=args.parent
#  overrides="
#  "

# class Results:
#   def __init__(self, success, timing=None):
#     self.success = success
#     self.timing = timing

def check_args_md(args):
  for arg in [args.nevents, args.run, args.subrun]:
    if arg is None:
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
  if args.nevents > 0:
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

def run_stage(stage, fcl, input_file, nevents, event, outname, artroot_out=False, tfile_out=False, do_timing=False):
  print('Running', stage)
  cmd = ['lar', '-c', fcl]

  if input_file is not None: cmd.append(input_file)

  cmd += ['-n', str(nevents),]
  cmd += ['--nskip', str(event),]

  if artroot_out:
    cmd += ['-o', outname]
  if tfile_out:
    tfile_outname = outname.replace('.root', '_tfile.root')
    cmd += ['-T', tfile_outname]
  
  if do_timing:    
    start_time = time.time()
  print(cmd)
  proc = subprocess.run(
    cmd, capture_output=True
  )
  if do_timing:
    end_time = time.time()

  print('Stdout:')
  print(proc.stdout.decode('utf-8'))
  print('Stderr:')
  print(proc.stderr.decode('utf-8'))
  
  if proc.returncode != 0:
    print('Error in processing', stage)
    print(proc.stderr.decode('utf-8'))
    sys.exit(proc.returncode)
  
  

  with open(outname.replace('.root', '.log'), 'w') as f:
    f.write(proc.stdout.decode('utf-8'))
  with open(outname.replace('.root', '.err'), 'w') as f:
    f.write(proc.stderr.decode('utf-8'))

  results = {
    'art_out':outname
  }
  if tfile_out:
    results['tfile_out'] = tfile_outname
  if do_timing:
    results['start_time'] = start_time
    results['end_time'] = end_time
  return results

def build_name(config, inputname=None):

  #TODO -- check inputname not none 
  base = (
    inputname.split('/')[-1].replace('.root', '').replace('.hdf5', '')
    if config['inherit_name']
    else config['art_out_base']
  )

  if config['justin_stage_outname']:
    base += f'_{os.getenv("JUSTIN_STAGE_ID", "1")}'
  if config['justin_jobid_outname']:
    jobid = os.getenv("JUSTIN_JOBSUB_ID", '1').split('@')[0].replace('.', '_')
    base += f'_{jobid}'
  if config['timestamp_outname']:
    base += f'_{int(time.time())}'
  base += '.root'
  return base

def build_config(config):
  defaults = {
    'time_last': False,
    'pass_name_through_stages': False,
    'timestamp_outname': False,
    'justin_stage_outname': False,
    'justin_jobid_outname': False,
    'inherit_name': False,
  }
  to_check = [
    'art_out_base',
  ]

  stage_defaults = {
    'art_out': False,
    'tfile_out': False,
    'make_art_metadata': False,
    'make_tfile_metadata': False,
    'nevents':-1, #DO we want this?
  }
  stage_to_check = [
    'fcl',
  ]
  
  for tc in to_check:
    if tc not in config:
      raise Exception(f'Error, need to provide {tc} in top level of yaml file')
  for key, val in defaults.items():
      if key not in config:
        config[key] = val

  for i, (stagename, stage) in enumerate(config['job_stages'].items()):
    for tc in stage_to_check:
      if tc not in stage:
        raise Exception(f'Error, need to provide {tc} in stage {stagename}')
    
    for key, val in stage_defaults.items():
      if key not in stage:
        stage[key] = val

    
def run_job(args):
  import yaml
  with open(args.yaml) as fin:
    config = yaml.safe_load(fin)
  build_config(config)

  time_last = config['time_last']
  #pass_name = config['pass_name_through_stages']
  

  stages = config['job_stages']
  n_stages = len(stages)
  input = args.i
  outname = build_name(config, args.i)
  past_fcls = []
  past_apps = []
  
  art_outputs = {}
  for i, (stagename, stage) in enumerate(stages.items()):
    print(stagename)

    if i == 0 and args.nevents is not None:
      stage['nevents'] = args.nevents

    if i == 0:
      stage['event'] = args.event
    else: stage['event'] = 0

    make_art_output = stage['art_out']
    make_tfile_output = stage['tfile_out']

    # if i > 0:
    outname = outname.replace('.root', f'_{stagename}.root')

    if i != (n_stages-1):
      past_fcls.append(stage['fcl'])
      past_apps.append(stagename)

    do_timing = time_last and (i == (n_stages-1))
    
    #TODO -- check args.i/input_from consistency
    input_from = stage['input_from']
    if input_from is None:
      input = None
    elif input_from != 'job':
      input = art_outputs[input_from]
    else:
      input = args.i
    result = run_stage(stagename, stage['fcl'], input,
                       stage['nevents'], stage['event'],
                       outname,
                       artroot_out=make_art_output,
                       do_timing=do_timing, tfile_out=make_tfile_output)
    print(result)
    input = outname
    end_fcl = stage['fcl']
    art_outputs[stagename] = result['art_out']

    if make_art_output and stage['make_art_metadata']:
      gen_fcl = None if 'gen' not in stages else stages['gen']['fcl']
      md_args = FakeArgs(args, f'{outname}.json', result['start_time'], result['end_time'], gen_fcl, end_fcl)
      md_args.past_fcls = past_fcls
      md_args.past_apps = past_apps
      md_args.nevents = get_artroot_nevents(outname)
      make_metadata(md_args)
    #TODO -- make tfile metadata configurable

  #Get metadata
  # Check the last stage if we're supposed store metadata -- TODO make this per stage
  # make_metadata = stage[]
  # if do_make_metadata:
  #   gen_fcl = None if 'gen' not in stages else stages['gen']['fcl']
  #   md_args = FakeArgs(args, f'{outname}.json', result['start_time'], result['end_time'], gen_fcl, end_fcl)
  #   md_args.past_fcls = past_fcls
  #   md_args.past_apps = past_apps
  #   md_args.nevents = get_artroot_nevents(outname)
  #   make_metadata(md_args)

def get_artroot_nevents(filename):
  import ROOT as RT
  f = RT.TFile.Open(filename)
  t = f.Get('Events')
  nevents = t.GetEntries()
  f.Close()
  return nevents

def get_artroot_nevents_routine(args):
  print(get_artroot_nevents(args.i))
if __name__ == '__main__':
  parser = ap()
  parser.add_argument(
    'routine',
    type=str,
    choices=['get_run_subrun', 'make_metadata', 'run_job', 'get_artroot_nevents'],
  )

  parser.add_argument('-i', type=str, default=None)
  parser.add_argument('--yaml', type=str, default=None)
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
    'run_job':run_job,
    'get_artroot_nevents':get_artroot_nevents_routine,
  }

  routines[args.routine](args)
