#!/bin/bash

#POSITIONAL_ARGS=()

NEVENTS=-1
OUTFILE="pduneana_ntuple.root"

while [[ $# -gt 0 ]]; do
  case $1 in
    -c)
      FCLFILE="$2"
      shift # past argument
      shift # past value
      ;;
    #-s|--searchpath)
    #  SEARCHPATH="$2"
    #  shift # past argument
    #  shift # past value
    #  ;;
    --samweb)
      DOSAMWEB="1"
      shift # past argument
      ;;
    -n|--nevents)
      NEVENTS="$2"
      shift
      shift
      ;;
    -o)
      OUTFILE="$2"
      shift
      shift
      ;;
    --nosetup)
      NOSETUP=YES
      shift # past argument
      ;;
    --dry_run)
      DRYRUN=1
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      FILE="$1" # save positional arg
      break
      ;;
  esac
done

if [ -z $NOSETUP ]; then
  echo "Will setup"
  #Setup general dune software
  source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh

  #Setup recent lar software suite
  setup dunesw v09_79_00d00 -q e26:prof
else  
  echo "Will not setup"
fi

if [ -z ${DOSAMWEB+x} ]; then
  echo "Will not samweb"
else  
  echo "Will samweb"
  FILE=$(samweb2xrootd ${FILE})
fi

if [ -z ${DRYRUN+x} ]; then
  lar -c $FCLFILE -T $OUTFILE -n $NEVENTS $FILE
else
  echo lar -c $FCLFILE -T $OUTFILE -n $NEVENTS $FILE
fi


