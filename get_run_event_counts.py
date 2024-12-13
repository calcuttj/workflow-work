from argparse import ArgumentParser as ap
from metacat.webapi import MetaCatClient
mc = MetaCatClient()
import numpy as np


if __name__ == '__main__':
    parser = ap()
    parser.add_argument('-r', '--runs', type=int, required=True, nargs='+')
    parser.add_argument('-o', type=str, default='run_events.txt')
    args = parser.parse_args()

    with open(args.o, 'w') as f:
        f.write('#run,event count,')
        for run in args.runs:
            res = mc.query(f'files where core.runs={run} and core.run_type=hd-protodune and core.data_tier=raw',
                        with_metadata=True)
            metadatas = [f['metadata'] for f in res]
            total_events = sum([md['core.event_count'] for md in metadatas])
            f.write(f'{run},{total_events}\n')
