# pnplab's bids-preproc pipeline

An fmriprep superlayer to handle hcp/cloud or distributed scheduling for heavy longitudinal datasets.

It does six things:
- abstract and orchestrate the preprocessing across any kind of distributed environments,
  it currently works with SLURM, but can easily be extended to any system, ie. ssh, amazon cloud, through any existing of the existing dask distributed/jobqueue implementation.
- provide an extra granularity to fmriprep at the level of session,
  as fmriprep currently only handles processing of either full dataset or of single participant at a time.
- wrap tool execution calls around docker or singularity virtual containers (or none at all), with the same source code.
- archive dataset with dar and only extract the relevant parts (ie. specific sessions or subjects) when needed on computing node for mutualised hypercomputing environments,
  as filesystem such as lustre, which we tested on the beluga cluster (compute canada):
  - cause fmriprep to randomly hang indefinitely due to process getting stuck in D-state mode (pending kernel-level state, likely due to the network filesystem drivers)
  - are slow (`seff` averages to 2.17% of CPU utilization for 92.36% for memory usage).
  - are limited in the amount of file one can write (the 1M default per-user scratch file count limit is already broken for a single dataset such as kaminati, when considering fmriprep intermediary generated files)
  and inner compute-node storage are too limited (a few hundreds gigs only) to store a single dataset, or even a single subject, with all fmriprep intermediary generated files (for kaminati).
- monkey patch broken fmriprep anatomic fast-track mechanism, which bugs on some dataset, cf. https://github.com/nipreps/smriprep/issues/224
- monitor unreachable dask workers (likely due to hypercomputer network congestion issues) and kill and reschedule the associated compute nodes when slurm is used.

The architecture should enable easy changes of the pipeline:
- ie. partially download the dataset, such as a single session, at the relevant time instead of extracting it from dar.
- ie. use a different orchestration system than slurm (for instance kubernetes, ..., basically anything, check both dask distributed and dask jobqueue documentations for all the options).

Before use:
- setup the license/freesurfer.txt file (get a license from freesurfer website and put it inside that file, cf. fmriprep doc)
- download the templateflow atlas using the script in `./scripts/download-templateflow-data.sh`
- download and build the relevant singularity images, which is only required if singularity is used.
  `mkdir ../singularity-images/; cd ../singularity-images/; singularity build bids-validator-1.8.5.simg docker://bids/validator:v1.8.5 ; singularity build fmriprep-20.2.6.simg docker://nipreps/fmriprep:20.2.6 ; singularity build smriprep-0.8.1.simg docker://nipreps/smriprep:0.8.1`.
  file `config.py` might have to be adapted to get the proper path

Sample usage:
- `python main.py --help`
- `python main.py --vm-engine docker --granularity subject --executor none --disable-mriqc '/scratch/nuks/kaminati-bids' '/scratch/nuks/kaminati-preproc'`
- `./preprocessing.sh --vm-engine singularity --granularity session --executor slurm --disable-mriqc --worker-memory-gb 64 --worker-cpu-count 16 --worker-count 7 --worker-walltime 2-12:00:00 --worker-local-dir '$SLURM_TMPDIR/pnplab-kaminati' '/scratch/nuks/kaminati-bids' '/scratch/nuks/kaminati-preproc'`
