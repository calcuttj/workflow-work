from argparse import ArgumentParser as ap
import subprocess

if __name__ == '__main__':
    parser = ap()
    parser.add_argument('--workflow', '-w', type=str, required=True)
    parser.add_argument('--scratch_files', type=str, required=True, nargs='+')
    parser.add_argument('--job_index', type=int, default=-6, help='Location in output file that holds the jobid')
    parser.add_argument('--delimiter', type=str, default='_')
    parser.add_argument('-o', type=str, default=None)

    args = parser.parse_args()

    proc = subprocess.run(['justin', 'show-jobs', '--workflow-id', args.workflow, '--state', 'finished'], capture_output=True)
    lines = proc.stdout.decode('utf-8').split('\n')
    jobids = []
    for l in lines:
        if 'finished' not in l: continue
        jobid = l.split('@')[0].replace('.', args.delimiter)
        print(jobid)
        jobids.append(jobid)
    
    files_jobids = {}
    for f in args.scratch_files:
        split = f.split(args.delimiter)
        jobid = args.delimiter.join(split[args.job_index:args.job_index+2])
        print(jobid)
        files_jobids[jobid] = f

    good_files = []
    for jobid, f in files_jobids.items():
        if jobid not in jobids: continue
        good_files.append(f+'\n')
    
    if args.o is not None:
        with open(args.o, 'w') as fout:
            fout.writelines(good_files)