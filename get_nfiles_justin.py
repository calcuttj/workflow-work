import subprocess
import sys
import os
from argparse import ArgumentParser as ap

def check_n(args):
  if args.n < 1:
    print('Error: need to supply -n greater than 0')
    exit(1) 

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('-n', type=int, help='Nfiles', default=1)
  parser.add_argument('-o', type=str, help='Output file for DIDs',
                      default='temp_did_file.txt')
  args = parser.parse_args()

  check_n(args)

  dids = []
  pfns = []
  for i in range(args.n):

    ## Run justin-get-file
    proc = subprocess.run(
      [f'{os.environ["JUSTIN_PATH"]}/justin-get-file'],
      capture_output=True,
    )

    ## If there's an error, then exit
    if proc.returncode != 0:
      print('Error in justin-get-file within get_nfiles_justin.py')
      print(proc.stdout)
      print(proc.stderr)
      print(proc.stderr, file=sys.stderr)
      exit(proc.returncode)

    ## If there's not an error, but an empty string was returned,
    ## then we're out of files and should get out of the loop
    if proc.stdout == '': break

    #Split and add did and pfn to list
    output = proc.stdout.decode('utf-8')
    dids.append(output.split()[0])
    pfns.append(output.split()[1])

  ## Print out pfns
  if len(pfns) == 0: print('')
  else:
    print(' '.join(pfns))
    
    ##Also write out the DIDs to a file 
    with open(args.o, 'w') as f:
      f.writelines([d + '\n' for d in dids])

  exit(0)
