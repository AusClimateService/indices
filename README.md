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
the easiest way to execute `run_icclim.py` is to run it at the command line
with the version of Python installed at: `/g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python`.
For example:
```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py -h
```

If you'd like to run the script/s in your own Python environment,
you'll need to install the following libraries using conda:
```
$ conda install -c conda-forge icclim cmdline_provenance gitpython
```

## Usage

Running the script at the command line with the `-h` option explains the user options,
including the very long list of indices that the script can calculate:

```bash
$ python run_icclim.py -h
```

```
usage: run_icclim.py [-h] [--input_files [INPUT_FILES ...]] [--variable VARIABLE] [--time_agg {min,mean,max}] [--start_date START_DATE]
                     [--end_date END_DATE] [--lat_bnds LAT_BNDS LAT_BNDS] [--lon_bnds LON_BNDS LON_BNDS] [--base_period BASE_PERIOD BASE_PERIOD]
                     [--slice_mode {year,month,DJF,MAM,JJA,SON,ONDJFM,AMJJAS}] [--verbose] [--local_cluster] [--nworkers NWORKERS] [--nthreads NTHREADS]
                     [--memory_limit MEMORY_LIMIT] [--dask_dir DASK_DIR] [--drop_time_bounds]
                     {tg,tn,tx,dtr,etr,vdtr,su,tr,wsdi,tg90p,tn90p,tx90p,txx,tnx,csu,gd4,fd,cfd,hd17,id,tg10p,tn10p,tx10p,txn,tnn,csdi,cdd,prcptot,rr1,sdii,cwd,rr,r10mm,r20mm,rx1day,rx5day,r75p,r75ptot,r95p,r95ptot,r99p,r99ptot,sd,sd1,sd5cm,sd50cm,cd,cw,wd,ww}
                     output_file

Command line program for calculating extremes indices.

positional arguments:
  {tg,tn,tx,dtr,etr,vdtr,su,tr,wsdi,tg90p,tn90p,tx90p,txx,tnx,csu,gd4,fd,cfd,hd17,id,tg10p,tn10p,tx10p,txn,tnn,csdi,cdd,prcptot,rr1,sdii,cwd,rr,r10mm,r20mm,rx1day,rx5day,r75p,r75ptot,r95p,r95ptot,r99p,r99ptot,sd,sd1,sd5cm,sd50cm,cd,cw,wd,ww}
                        index name
  output_file           output file name

options:
  -h, --help            show this help message and exit
  --input_files [INPUT_FILES ...]
                        input files for a particular variable
  --variable VARIABLE   variable to process from input files
  --time_agg {min,mean,max}
                        temporal aggregation to apply to input files (used to convert hourly to daily)
  --start_date START_DATE
                        Start date in YYYY, YYYY-MM or YYYY-MM-DD format
  --end_date END_DATE   Start date in YYYY, YYYY-MM or YYYY-MM-DD format
  --lat_bnds LAT_BNDS LAT_BNDS
                        Latitude bounds
  --lon_bnds LON_BNDS LON_BNDS
                        Longitude bounds
  --base_period BASE_PERIOD BASE_PERIOD
                        Base period (for percentile calculations) in YYYY-MM-DD format
  --slice_mode {year,month,DJF,MAM,JJA,SON,ONDJFM,AMJJAS}
                        Sampling frequency for index calculation [default=year]
  --verbose             Set logging level to INFO
  --local_cluster       Use a local dask cluster
  --nworkers NWORKERS   Number of workers for local dask cluster
  --nthreads NTHREADS   Number of threads per worker for local dask cluster
  --memory_limit MEMORY_LIMIT
                        Memory limit for local dask cluster
  --dask_dir DASK_DIR   Directory where dask worker space files can be written. Required for local dask cluster.
  --drop_time_bounds    Drop the time bounds from output file
```

#### Example 1: Simple indices

The most basic use of `run_icclim.py` requires passing the script 
the name of the index to calculate,
the name of the output file,
the name/s of the input file/s, and
the name of the variable to access from that file/s,
For example:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py txx /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/climdex/txx/txx_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_year_197901-200112.nc --input_files /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/output/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/day/tasmax/tasmax_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_day_*.nc --variable tasmax --verbose
```

In the example above,
the `--verbose` flag has also been invoked so that the program prints its progress to the screen.

#### Example 2: Sub-daily input data

All the icclim climate indices are calculated from daily data.
If you want to input sub-daily (e.g. hourly) data instead,
you need to use the `--time_agg` option to specify how to temporally aggregate the data.
For instance,

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py tnn /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/GLOBAL-gn/none/ECMWF-ERA5/evaluation/r1i1p1f1/none/none/climdex/tnn/tnn_AUS-gn_ECMWF-ERA5_evaluation_r1i1p1f1_year_195901-202112.nc --input_files /g/data/rt52/era5/single-levels/reanalysis/2t/*/*.nc --variable t2m --time_agg min --start_date 1959-01-01 --end_date 2021-12-31 --lon_bnds 111.975 156.275 --lat_bnds -44.525 -9.975 --verbose
```

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py r10mm /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/GLOBAL-gn/none/ECMWF-ERA5/evaluation/r1i1p1f1/none/none/climdex/r10mm/r10mm_AUS-gn_ECMWF-ERA5_evaluation_r1i1p1f1_year_195901-202112.nc --input_files /g/data/rt52/era5/single-levels/reanalysis/mtpr/*/*.nc --variable mtpr --time_agg mean --start_date 1959-01-01 --end_date 2021-12-31 --lon_bnds 111.975 156.275 --lat_bnds -44.525 -9.975 --verbose
```

In both these examples the `--start_date`, `--end_date`, `--lat_bnds` and `--lon_bnds` options have been used to subset the data.
Selecting a latitude / longitude box around Australia rather than processing the whole globe
(this example uses the AGCD dataset spatial bounds) can be particularly useful.
If the TNn example above is run without the `--lat_bnds` and `--lon_bnds` selection
it takes several hours to process the whole ERA5 global grid
(when given 120GB of RAM on the Gadi "hugemem" job queue).
With the lat/lon subsetting it takes less than an hour.

#### Example 3: Multi-variate indices

For a bivariate index like dtr (mean diurnal temperature range),
you need to use invoke the `--input_files` and `--variable` options twice.
The first time to specify the maximum temperature files and variable name,
and the second time to specify the minimum temperature files and variable name.
For example:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py dtr /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-r005/none/BOM-AGCD/historical/v1/none/none/climdex/dtr/dtr_AUS-r005_BOM-AGCD_historical_v1_year_191001-202112.nc  --input_files /g/data/xv83/agcd-csiro/tmax/daily/tmax_AGCD-CSIRO_r005_*.nc --variable tmax --input_files /g/data/xv83/agcd-csiro/tmin/daily/tmin_AGCD-CSIRO_r005_*.nc --variable tmin --start_date 1910-01-01 --end_date 2021-12-31 --verbose
```

> **CF-compliance**
>
> The icclim library (because of the xclim library it builds upon) requires CF-compliant variable attributes
> (see the [health checks and metadata attributes](https://xclim.readthedocs.io/en/stable/notebooks/usage.html#Health-checks-and-metadata-attributes)
> section of the xclim docs for details).
> The `run_icclim.py` script has metadata fixing capability built in that should work for most datasets
> so that users don't have to edit their files before passing them to `run_icclim.py`.
> If you pass the script a file that does cause a metadata-related error,
> you can start a new issue [here](https://github.com/AusClimateService/indices/issues)
> describing the error and we can add appropriate metadata handling to fix the problem.


#### Example 4: "Along the time axis" indices

A small number of climate indices require calculations to be performed along the entire time axis.
For instance, in the example below the `r95ptot` index requires calculation of the 95th percentile
along the time axis for the entire 1900-2021 period:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py r95ptot /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-r005/none/BOM-AGCD/historical/v1/none/none/climdex/r95ptot/r95ptot_AUS-r005_BOM-AGCD_historical_v1_year_190001-202112.nc --input_files /g/data/xv83/agcd-csiro/precip/daily/precip-total_AGCD-CSIRO_r005_19000101-20220405_daily_space-chunked.zarr --variable precip --start_date 1900-01-01 --end_date 2021-12-31 --verbose
```

Indices that involve calculations along the entire time axis are much more memory intensive.
The example above used about 115 GB of RAM on the Australian Research Environment at NCI and took about 15 minutes to run.

The icclim documentation has [extensive notes](https://icclim.readthedocs.io/en/stable/how_to/dask.html)
on how to improve performance for indices like this via data chunking and parallelisation using dask. 
The `run_icclim.py` script has a `--local_cluster` option that can be used
(in conjunction with the `--nworkers` and `--nthreads` options)
to launch and configure a local dask cluster.
We are still figuring out the optimal local cluster settings for different use cases
and will add advice to this documentation soon,
but in general the computation will be fastest if you simply allocate a large amount of memory to the job
(e.g. using the `largemem` job queue on NCI)
as opposed to fiddling around with a local dask cluster.
