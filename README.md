# indices

This directory contains a command line program for calculating climate indices
such as those used by the [ETCCDI](http://etccdi.pacificclimate.org/list_27_indices.shtml) and
[Climdex](https://www.climdex.org/) projects.

The program makes use of the [icclim](https://icclim.readthedocs.io/en/stable/) library.
Detailed descriptions of each index are available in the
icclim [API](https://icclim.readthedocs.io/en/stable/references/ecad_functions_api.html#module-icclim._generated_api)
and an associated [ATBD](https://www.ecad.eu/documents/atbd.pdf).

## Python environment

If you're a member of the `xv83` project on NCI
(i.e. if you're part of the Australian Climate Service),
the easiest way to execute `run_icclim.py` is to run it with the version of Python
installed at: `/g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python`.
For example,
```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py -h
```

If you'd like to run the script/s in your own Python environment,
you'll need to install the following libraries using conda...
```
conda install -c conda-forge icclim cmdline_provenance gitpython dask
```

## Usage

Running the script at the command line with the `-h` option explains the user options,
including the very long list of indices that the script can calculate.

```bash
$ python run_icclim.py -h
```

```
usage: run_icclim.py [-h] [--time_period TIME_PERIOD TIME_PERIOD] [--base_period BASE_PERIOD BASE_PERIOD]
                     [--slice_mode {year,month,DJF,MAM,JJA,SON,ONDJFM,AMJJAS}] [--dataset {AGCD}] [--drop_time_bounds] [--memory_vis] [--verbose]
                     [--local_cluster] [--nworkers NWORKERS] [--dask_dir DASK_DIR]
                     [input_files ...]
                     {tg,tn,tx,dtr,etr,vdtr,su,tr,wsdi,tg90p,tn90p,tx90p,txx,tnx,csu,gd4,fd,cfd,hd17,id,tg10p,tn10p,tx10p,txn,tnn,csdi,cdd,prcptot,rr1,sdii,cwd,rr,r10mm,r20mm,rx1day,rx5day,r75p,r75ptot,r95p,r95ptot,r99p,r99ptot,sd,sd1,sd5cm,sd50cm,cd,cw,wd,ww} var_name output_file

Command line program for calculating extremes indices.

positional arguments:
  input_files           input files
  var_name              variable name

{tg,tn,tx,dtr,etr,vdtr,su,tr,wsdi,tg90p,tn90p,tx90p,txx,tnx,csu,gd4,fd,cfd,hd17,id,tg10p,tn10p,tx10p,txn,tnn,csdi,cdd,prcptot,rr1,sdii,cwd,rr,r10mm,r20mm,rx1day,rx5day,r75p,r75ptot,r95p,r95ptot,r99p,r99ptot,sd,sd1,sd5cm,sd50cm,cd,cw,wd,ww} 
                        index name
  output_file           output file name

options:
  -h, --help            show this help message and exit
  --time_period TIME_PERIOD TIME_PERIOD
                        Time period in YYYY-MM-DD format
  --base_period BASE_PERIOD BASE_PERIOD
                        Base period (for percentile calculations) in YYYY-MM-DD format
  --slice_mode {year,month,DJF,MAM,JJA,SON,ONDJFM,AMJJAS}
                        Sampling frequency for index calculation
  --dataset {AGCD}      Apply dataset and variable specific metadata fixes for CF compliance
  --drop_time_bounds    Drop the time bounds from output file
  --memory_vis          Visualise memory and CPU usage (creates profile.html)
  --verbose             Set logging level to INFO
  --local_cluster       Use a local dask cluster
  --nworkers NWORKERS   Number of workers for cluster [default lets dask decide]
  --dask_dir DASK_DIR   Directory where dask worker space files can be written. Required for local cluster.
```

#### Example 1: Simple index, CF-compliant file metadata

The most basic use of `run_icclim.py` is to calculate a simple index
from files whose variable attributes are CF-compliant.
This basic usage simply requires passing the script the name/s of the input file/s,
the name of the variable to access from that file/s,
the name of the index to calculate,
and the name of the output file:

```
/g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/output/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/day/tasmax/tasmax_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_day_*.nc tasmax txx /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/climdex/txx/txx_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_year_197901-200112.nc --verbose
```

In the example above,
the `--verbose` flag has also been invoked so that the program prints its progress to the screen.
For simple indexes like TXx there's typically no need to use a local dask cluster.

#### Example 2: Non-CF-compliant input file

The icclim library (because of the xclim library it builds upon) requires CF-compliant variable attributes
(see the [health checks and metadata attributes](https://xclim.readthedocs.io/en/stable/notebooks/usage.html#Health-checks-and-metadata-attributes)
section of the xclim docs for details).
The `run_icclim.py` script has a metadata fixing capability built in for particular datasets (using the `--dataset` option)
so that users don't have to edit their files before passing them to `run_icclim.py`.
For example, in this example `--dataset AGCD` is used to correct the attributes of AGCD input files.

```
/g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py /g/data/xv83/agcd-csiro/precip/daily/precip-total_AGCD-CSIRO_r005_*_daily.nc precip r10mm /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-r005/none/BOM-AGCD/historical/v1/none/none/climdex/r10mm/r10mm_AUS-r005_BOM-AGCD_historical_v1_year_190001-202112.nc --verbose --dataset AGCD
```

If you'd like metadata handling added for other datasets you can start a new issue/request
[here](https://github.com/AusClimateService/indices/issues).

#### Example 3: Complex index

In this context a "complex index" is one that requires calculation along the entire time axis.
For instance, in the example below the `r95ptot` index requires calculation of the 95th percentile
along the entire time axis of the 30 year base period.

```
/g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python /home/599/dbi599/indices/run_icclim.py precip-total_AGCD-CSIRO_r005_19000101-20220405_daily_space-chunked.zarr precip r95ptot /g/data/xv83/dbi599/indices/r95ptot_year_AGCD_v1_r005_1910-2021.nc --time_period 1900-01-01 2021-12-31 --base_period 1961-01-01 1990-12-31 --dataset AGCD --verbose
```

Indices that involve calculations along the entire time axis are much more memory intensive
and at the moment we are still experimenting on how to solve this problem.
The example above uses a data file chunked along spatial (as opposed to temporal) axes
and we are experimenting with using the `--local_cluster` option to parallelise the calculation,
but we are still working on an optimal solution.

