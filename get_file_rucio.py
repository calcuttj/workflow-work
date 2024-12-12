from rucio.client.replicaclient import ReplicaClient
from argparse import ArgumentParser as ap

all_rses = {
  'FNAL':'url'
}


def list_rses(args):
  print('Available RSE names:')
  for k in all_rses.keys(): print(k)

def get_replica(args):
  rc = ReplicaClient()

  dids = [
      {'scope':f.split(':')[0], 'name':f.split(':')[1]}
      for f in args.f
  ]

  for rep in rc.list_replicas(dids, schemes='pfns'):
    print(rep)

if __name__ == '__main__':
  parser = ap() #TODO -- add help
  parser.add_argument(
    'command',
    type=str,
    choices=[
      'list-rses',
      'get-replica',
    ],
  )
  parser.add_argument('-f', type=str, default=None, nargs='+',
                      help='File(s) to find')

  args = parser.parse_args()

  commands = {
    'list-rses':list_rses,
    'get-replica':get_replica,
  }

  commands[args.command](args)
