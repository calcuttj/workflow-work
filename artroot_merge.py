from argparse import ArgumentParser as ap
from metacat.webapi import MetaCatClient
from rucio.client import Client as RucioClient
import subprocess
import json

rc = RucioClient()
mc = MetaCatClient()


def do_list(query):
    print('Getting', query)
    files = [[f['fid'], int(f['size']*1e-6)] for f in mc.query(query)]
    
    files.sort(key=lambda x : x[1])
    for f in files:
        print(f)
    if args.o is not None:
        with open(args.o, 'w') as fout:
            fout.writelines([f'{f[0]} {f[1]}\n' for f in files])

def combine_list(the_list):
    with open(args.i, 'r') as fin:
        files_sizes = [l.strip().split() for l in fin.readlines()]
    files_sizes = [[l[0], int(l[1])] for l in files_sizes]
    files_sizes.sort(key=lambda x : x[-1])
    print(files_sizes)

    combined = []

    while len(files_sizes) > 0:
        #Get from the back first -- this will be the largest
        back_file = files_sizes.pop()
        combined.append([[back_file[0]], back_file[1]])
        
        while combined[-1][1] < 700:
            if len(files_sizes) > 0:
                back_file = files_sizes.pop()
                combined[-1][0].append(back_file[0])
                combined[-1][1] += back_file[1]
            else: break #TODO -- WARNING
    print(combined)

    if args.o is not None:
        with open(args.o, 'w') as fout:
            fout.writelines(','.join(l[0]) + ':' + str(l[1]) + '\n' for l in combined)
def routine(args):
    if args.routine == 'list':
        do_list(args.query)
    elif args.routine == 'combine_list':
        combine_list(args.i)
    elif args.routine == 'merge':
        if args.fcl is None: raise Exception('NEED FCL')
        if args.o is None: raise Exception('NEED Output name')
        if args.fids is None and args.fid_list is None:
            raise Exception('NEED FIDS OR FID_LIST')
        
        if args.fids is not None and len(args.fids) != 0:
            fids = args.fids
        else:
            with open(args.fid_list, 'r') as f:
                fids = f.readlines()[0].strip().split(':')[0].split(',')
            
        do_merge(fids, args.fcl, args.o, skip_checksum=args.skip_checksum)

def do_merge(fids, fcl, outname, skip_checksum=False):
    
    files = [mc.get_file(fid=fid, with_metadata=True) for fid in fids]
    metadatas = [f['metadata'] for f in files]
    namespaces = list(set([f['namespace'] for f in files]))
    if len(namespaces) != 1:
        raise Exception('ERROR DIFFERING NAMESPACES BEING MERGED')
    files = [{'scope':f['namespace'], 'name':f['name']} for f in files]
    
    print(files)
    to_merge = []
    for f in files:

        replicas = [i for i in rc.list_replicas([f], rse_expression=args.rse)]
    
        #TODO figure out the real logic here
        if len(replicas) == 0:
            raise Exception('COULD NOT FIND FILE')
        
        #Get the first replica
        rep = replicas[0]
        pfns = rep['pfns']
        for key in pfns.keys():
            print(key)
            to_merge.append(key)
            break
    print(f'Got {len(to_merge)} pfns from {len(files)} input')
    if len(to_merge) != len(files):
        raise Exception('COULD NOT FIND THE RIGHT NUMBER OF PFNS')

    merged_md = dict()

    as_sets = [
        'core.runs',
        'core.runs_subruns',
    ]
    combine_fields = [
        'core.event_count',
    ]

    for field in as_sets:
        if field not in merged_md:
            merged_md[field] = []
        for md in metadatas:
            merged_md[field] += md[field]
        print(merged_md[field])
        merged_md[field] = list(set(merged_md[field]))
    print(merged_md)
    
    for field in combine_fields:
        merged_md[field] = 0
        for md in metadatas:
            merged_md[field] += md[field]


    unique = [
        'beam.momentum',
        'beam.polarity',
        'core.application',
        'core.application.family',
        'core.application.name',
        'core.application.version',
        'core.data_stream',
        'core.data_tier',
        'core.file_format',
        'core.file_type',
        'core.group',
        'core.run_type',
        'dune.campaign',
        'dune.config_file',
        'dune.output_status',
        'dune.requestid',
        'dune_mc.detector_type',
        'dune_mc.electron_lifetime',
        'dune_mc.gen_fcl_filename',
        'dune_mc.generators',
        'dune_mc.liquid_flow',
        'dune_mc.space_charge',
        'dune_mc.with_cosmics',

    ]

    for field in unique:
        merged_md[field] = []
        for md in metadatas:
            merged_md[field].append(md[field])
        merged_md[field] = list(set(merged_md[field]))
        if len(merged_md[field]) != 1:
            raise Exception(f'NON-UNIQUE METADATA FIELD {field}\n\t{" ".join(merged_md[field])}')
        merged_md[field] = merged_md[field][0]


    #Special origin check
    origin_name = 'origin.applications.names'
    origin_config = 'origin.applications.config_files'
    origin_version = 'origin.applications.versions'

    origins = [origin_name, origin_config, origin_version]
    n_present = {k:0 for k in origins}
    n_missing = {k:0 for k in origins}
    for md in metadatas:
        #check if they're there
        for k in origins:
            if k in md: n_present[k] += 1
            else: n_missing[k] += 1
    for k in origins:
        if n_present[k] > 0 and n_missing[k] > 0:
            raise Exception('Files have mismatched origin info')
    
    if n_present[origin_name] > 0:
        print('Checking origin')
        n_names = -1
        n_configs = -1
        n_versions = -1
        for md in metadatas:
            if n_names < 0:
                n_names = len(md[origin_name])
                n_configs = len(md[origin_config])
                n_versions = len(md[origin_version])

                base_versions = md[origin_version]
                base_names = md[origin_name]
                base_configs = md[origin_config]
                continue
            if ((len(md[origin_name]) != n_names) or ((len(md[origin_config]) != n_configs)) or
                (len(md[origin_version]) != n_versions)):
                raise Exception('Differing numbers of origin apps')
            
            names_good = True
            for n1, n2 in zip(base_names, md[origin_name]):
                if n1 != n2:
                    names_good = False
                    break
            if not names_good: raise Exception('Origin names differ')

            for k, v1 in base_versions.items():
                if k not in md[origin_version]:
                    raise Exception(f'Missing origin version for {k}')
                v2 = md[origin_version][k]
                if v1 != v2:
                    raise Exception(f'Mismatched origin versions for {k}: {v1} {v2}')
            for k, v1 in base_configs.items():
                if k not in md[origin_config]:
                    raise Exception(f'Missing origin config for {k}')
                v2 = md[origin_config][k]
                if v1 != v2:
                    raise Exception(f'Mismatched origin configs for {k}: {v1} {v2}')
        merged_md[origin_name] = base_names
        merged_md[origin_config] = base_configs
        merged_md[origin_version] = base_versions

    for k, v in merged_md.items():
        print(k, v)
    
    namespace = namespaces[0]
    


    #do the merging
    cmd = ['lar', '-c', fcl, '-o', outname, *to_merge]
    print('WILL RUN')
    print(cmd)
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        raise Exception('ERROR MERING', proc.returncode, proc.stderr)
    merged_md['dune.dataset_name'] = f'{namespace}:{namespace}_{outname.replace(".root", "")}'
    #Write out metadata
    final_md = {
        'metadata':merged_md,
        'namespace':namespace,
        'name':outname,
    }
    final_md = finish_metadata(outname, final_md, skip_checksum=skip_checksum)
    with open(args.o + '.json', 'w') as f:
        json.dump(final_md, f, indent=2)


def finish_metadata(outname, results, skip_checksum=False):
    print('Finishing metadata')

    import os
    results['size'] = os.path.getsize(outname)

    if not skip_checksum:
      proc = subprocess.run(['xrdadler32', outname], capture_output=True)
      if proc.returncode != 0:
        raise Exception('xrdadler32 failed', proc.returncode, proc.stderr)

      checksum = proc.stdout.decode('utf-8').split()[0]
      results['checksums'] = {'adler32':checksum}

    return results


if __name__ == '__main__':
    parser = ap()
    parser.add_argument('--query', '-q', type=str, default=None)
    parser.add_argument('-i', type=str, default=None)
    parser.add_argument('--fids', type=str, nargs='+')
    parser.add_argument('--fid_list', type=str, default=None)
    parser.add_argument('-n', type=int, help='line in fid_list to merge', default=0)
    parser.add_argument('-o', type=str, default=None)
    parser.add_argument('--rse', type=str, default=None)
    parser.add_argument('--fcl', '-c', type=str, default=None)
    parser.add_argument('--skip_checksum', action='store_true')
    parser.add_argument('routine', type=str, default='list',
                        choices=['list', 'combine_list', 'merge'])
    args = parser.parse_args()
    

    routine(args)