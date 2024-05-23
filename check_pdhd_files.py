from metacat.webapi import MetaCatClient
from argparse import ArgumentParser as ap

def get_pfns(files, index=-9):
  return [int(f['name'].split('_')[index]) for f in files]

def get_counts(pfns):
  return {i:pfns.count(i) for i in set(pfns)}

def print_counts(counts):
  for i, j in counts.items():
    if j > 1: print(i, j)

def build_queries(args):
  sce_on_reco_query = (
    f"files where dune.workflow['workflow_id'] in ({args.w}) "
    ' and dune.output_status=confirmed and dune_mc.space_charge=yes '
    ' and core.data_tier=full-reconstructed'
  )
  sce_on_pandora_query = sce_on_reco_query.replace('full-reconstructed', 'pandora_info')
  sce_off_reco_query = sce_on_reco_query.replace(
      'space_charge=yes', 'space_charge=no')
  sce_off_pandora_query = sce_on_pandora_query.replace(
      'space_charge=yes', 'space_charge=no')
  return {
    'sce_on_pandora':sce_on_pandora_query,
    'sce_off_pandora':sce_off_pandora_query,
    'sce_on_reco':sce_on_reco_query,
    'sce_off_reco':sce_off_reco_query,
  }

if __name__ == '__main__':

  parser = ap()
  parser.add_argument('-w', type=str, required=True)
  #parser.add_argument('--pandora', action='store_true')
  args = parser.parse_args()

  queries = build_queries(args)
  sce_on_pandora_query = queries['sce_on_pandora']
  sce_off_pandora_query = queries['sce_off_pandora']
  sce_on_reco_query = queries['sce_on_reco']
  sce_off_reco_query = queries['sce_off_reco']

  mc = MetaCatClient()
  #sce_off_pandora_files = mc.query(sce_off_pandora_query)
  #sce_on_pandora_files = mc.query(sce_on_pandora_query)
  sce_off_reco_files = mc.query(sce_off_reco_query)
  sce_on_reco_files = mc.query(sce_on_reco_query)

  #sce_off_pandora_pfns = get_pfns(sce_off_pandora_files, -11)
  #sce_on_pandora_pfns = get_pfns(sce_on_pandora_files, -11)
  sce_off_reco_pfns = get_pfns(sce_off_reco_files)
  sce_on_reco_pfns = get_pfns(sce_on_reco_files)

  #sce_off_pandora_counts = get_counts(sce_off_pandora_pfns)
  #sce_on_pandora_counts = get_counts(sce_on_pandora_pfns)
  sce_off_reco_counts = get_counts(sce_off_reco_pfns)
  sce_on_reco_counts = get_counts(sce_on_reco_pfns)

  print('All PFNs SCE On, Reco:', len(sce_on_reco_pfns))
  print('Unique PFNs SCE On, Reco:', len(set(sce_on_reco_pfns)))
  print()
  print('All PFNs SCE Off, Reco:', len(sce_off_reco_pfns))
  print('Unique PFNs SCE Off, Reco:', len(set(sce_off_reco_pfns)))
  print()
  #print('All PFNs SCE On, Pandora:', len(sce_on_pandora_pfns))
  #print('Unique PFNs SCE On, Pandora:', len(set(sce_on_pandora_pfns)))
  #print()
  #print('All PFNs SCE Off, Pandora:', len(sce_off_pandora_pfns))
  #print('Unique PFNs SCE Off, Pandora:', len(set(sce_off_pandora_pfns)))
  print()
  print('Multiple SCE On, Reco:')
  print_counts(sce_on_reco_counts)
  print()
  print('Multiple SCE Off, Reco:')
  print_counts(sce_off_reco_counts)
  print()
  #print('Multiple SCE On, Pandora:')
  #print_counts(sce_on_pandora_counts)
  #print()
  #print('Multiple SCE Off, Pandora:')
  #print_counts(sce_off_pandora_counts)
  #print()


  all_pfns = {
    #'SCE On Pandora':sce_on_pandora_pfns,
    #'SCE Off Pandora':sce_off_pandora_pfns,
    'SCE On Reco':sce_on_reco_pfns,
    'SCE Off Reco':sce_off_reco_pfns,
  }
  for i,pfns_i in all_pfns.items():
    for j, pfns_j in all_pfns.items():
      if i == j: continue
      print(i, '------', j)
      for pfn in pfns_i:
        if pfn not in pfns_j:
          print(i, pfn, 'Not in', j)
