from argparse import ArgumentParser as ap
import math
import subprocess

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
    for iline, l in enumerate(lines):
        if args.max > 0 and iline >= args.max: break
        f = l[0]
        nsplits = int(l[1])
        nsubs = int(l[2])
        run = f.split('_')[-2]
        # print(f, run, nsplits, nsubs)
        persplit=10 #TODO -- make config
        for j in range(nsubs):
            cmd = [
                'fife_launch', '-c', args.c, f'-Oglobal.split={nsplits}',
                               f'-Oglobal.start={j*persplit}',
                               f'-Oglobal.extra_dir=test_{run}',
                               f'-Oglobal.run={run}',
                               f'-Oglobal.n={persplit}',
                               f'-Oglobal.input_file={f}',
            ]
            if args.verbose or args.dry_run:
                print(cmd)
            if not args.dry_run:
                subprocess.run(cmd)
            

if __name__ == '__main__':
    parser = ap()
    parser.add_argument('routine', type=str, choices=['split', 'submit', 'count'])
    parser.add_argument('-i', type=str, nargs='+')
    parser.add_argument('-o', type=str, default=None)
    parser.add_argument('-c', type=str, default=None)
    parser.add_argument('--max', type=int, default=10)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--dry_run', action='store_true', help='Just ')
    #parser.add_argument

    args = parser.parse_args()
    routines = {
        'split':split,
        'count':count,
        'submit':submit,
    }
    routines[args.routine](args)
