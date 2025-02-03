from argparse import ArgumentParser as ap
import math
import subprocess
from condb2 import ConDB, ConDBClient
import time
from glob import glob as ls

def distribute(nlines, nsplits):
    base, extra = divmod(nlines, nsplits)
    return [base + (i < extra) for i in range(nsplits)]

def count(args):
    print(f'will check {len(args.i)} files')
    proc = subprocess.run(['wc', '-l', *args.i], capture_output=True)
    print(f'done checking')
    results = [l.strip().split() for l in proc.stdout.decode('utf-8').split('\n')]
    results = [[r[1], int(r[0])] for r in results if len(r) > 0 and r[1] != 'total']
    return results


def split(args):
    lines = count(args)
    if args.o is not None:
        fout = open(args.o, 'w')
        fout_low = open(f'low_count_{args.o}', 'w')
    for l in lines:

        nfiles = l[1]
        if nfiles < 50:
            fout_low.write(f'{l[0]}:{l[1]}\n')
        else:
            remainder = (nfiles % 100)
            if remainder > 50:
                nsplits = math.ceil(nfiles / 100.)
            else:
                nsplits = int(nfiles/100.)

            splits = distribute(nfiles, nsplits)

            print(l, 'nsplits:', nsplits, 'splits:', splits)
            
            nsubs = math.ceil(nsplits / args.max)

            if args.o is not None:
                fout.write(f'{l[0]}:{nsplits}:{nsubs}\n')
    if args.o is not None: fout.close()

def submit(args):
    # TODO -- check args.i len = 1
    with open(args.i[0], 'r') as f:
        lines = [l.strip().split(':') for l in f.readlines()]
    
    if not args.dry_run:
        stamp = int(time.time()*100)
        fsummary = open(f'submission_summary_{stamp}.csv', 'w')
        fsummary.write(f'## Input file: {args.i[0]}\n')
        fsummary.write('extra_dir,nsplits,start,run,persplit,input_file,jobid\n')

    for iline, l in enumerate(lines[args.submit_start:]):
        if args.submit_lines > 0 and iline >= args.submit_lines: break
        f = l[0]
        nsplits = int(l[1])
        nsubs = int(l[2])
        run = f.split('_')[args.index]
        # print(f, run, nsplits, nsubs)
        persplit=10 #TODO -- make config
        for j in range(nsubs):
            cmd = [
                'fife_launch', '-c', args.c, f'-Oglobal.split={nsplits}',
                               f'-Oglobal.start={j*persplit}',
                               f'-Oglobal.extra_dir={args.extra_dir}_{run}',
                               f'-Oglobal.run={run}',
                               f'-Oglobal.n={persplit}',
                               f'-Oglobal.input_file={f}',
                               f'-Oglobal.stream={args.stream}'
            ]

            if args.verbose or args.dry_run:
                print(' '.join(cmd))
            if not args.dry_run:
                proc = subprocess.run(cmd, capture_output=True)

                print(proc.stdout.decode('utf-8'))
                print(proc.stderr.decode('utf-8'))
                subout = proc.stdout.decode('utf-8').split('\n')
                for line in subout:
                    if 'Use job id' in line:
                        jobid = line.split()[3]
                fsummary.write(f'{args.extra_dir}_{run},{nsplits},{j*persplit},{run},{persplit},{f},{jobid}\n')
    if not args.dry_run:
        print(f'Wrote submission_summary_{stamp}.csv')
        fsummary.close()

def sort_low(args):
    if args.index is None: raise Exception('Need to provide --index')
    with open(args.i[0]) as f:
        lines = [l for l in f.readlines()]
    lines.sort(key = lambda l : int(l.split('_')[args.index]))
    with open(args.o, 'w') as fout:
        fout.writelines(lines)

def combine_low(args):
    with open(args.i[0], 'r') as f:
        lines = [
            [int(i) for i in l.strip().split(',')]
            for l in f.readlines()
            if '#' not in l
        ]
    print(lines)
    check_continuity(lines)
    if lines[0][0] != 1:
        raise Exception('Error need to start at 1')

    with open(args.i[1], 'r') as f:
        all_runs = [l.strip() for l in f.readlines()]
    

    if lines[-1][1] != len(all_runs):
        raise Exception('Error. Need to end on the number of low-count runs:'
                        f' {lines[-1][1]} {len(all_runs)}')
    
    for ichunk, chunk in enumerate(lines):
        print(f'Combining chunks {chunk}')
        chunk_paths = []
        # runs = []
        for i in range(chunk[0]-1, chunk[1]):
            filename = all_runs[i-1].split(':')[0]
            # runs.append(filename.split('_')[args.index])
            
            with open(filename, 'r') as fin:
                chunk_paths += fin.readlines()
        with open(f'combined_chunk{ichunk}_{args.o}', 'w') as fout:
            fout.writelines(chunk_paths)
        


def check_continuity(chunks):
    for i in range(1, len(chunks)):
        if chunks[i-1][1] != (chunks[i][0] - 1):
            raise Exception('Error. Found noncontinuity', chunks[i-1], chunks[i])

def check_output(args):
    with open(args.i[0], 'r') as f:
        lines = [l.strip().split(',') for l in f.readlines() if '#' not in l and 'extra_dir' not in l]
    
    nexpected = dict()
    nfiles = dict()
    for line in lines:
        thedir = f"{args.extra_dir}/{line[0]}/{line[-1].split('@')[0].replace('.','_')}/"
        print(thedir)
        files = ls(thedir + '*root')
        print(len(files))
        if line[0] not in nfiles:
            nfiles[line[0]] = 0
        if line[0] not in nexpected:
            nexpected[line[0]] = int(line[1])
        nfiles[line[0]] += len(files)

        
    print('Writing')
    with open(args.o, 'w') as fout:
        for thedir, n in nfiles.items():
            nexp = nexpected[thedir]
            fout.write(f"{thedir},{n},{nexp}\n")
            if n != nexp: print('WARNING: FOUND MISMATCH IN', thedir, n, nexp)
    
if __name__ == '__main__':
    parser = ap()
    parser.add_argument('routine', type=str, choices=['split', 'submit',
                                                      'count', 'sort_low',
                                                      'combine_low', 'check_output'])
    parser.add_argument('-i', type=str, nargs='+')
    parser.add_argument('-o', type=str, default=None)
    parser.add_argument('-c', type=str, default=None)
    parser.add_argument('--max', type=int, default=10)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--dry_run', action='store_true', help='Just ')
    parser.add_argument('--extra_dir', type=str, default='test')
    parser.add_argument('--index', type=int, default=None)
    parser.add_argument('--submit_start', type=int, default=0)
    parser.add_argument('--submit_lines', type=int, default=-1)
    parser.add_argument('--stream', type=str, default='cosmics', choices=['physics', 'cosmics'])
    #parser.add_argument

    args = parser.parse_args()
    routines = {
        'split':split,
        'count':count,
        'submit':submit,
        'sort_low':sort_low,
        'combine_low':combine_low,
        'check_output':check_output,
    }
    routines[args.routine](args)
