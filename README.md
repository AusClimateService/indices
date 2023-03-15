# indices

This directory contains two command line programs for calculating climate indices.

`run_icclim.py` uses the [icclim](https://icclim.readthedocs.io/en/stable/) library
to calculate simple indices such as those used by the
[ETCCDI](http://etccdi.pacificclimate.org/list_27_indices.shtml) and
[Climdex](https://www.climdex.org/) projects.
Detailed descriptions of each index are available in the
icclim [API](https://icclim.readthedocs.io/en/stable/references/ecad_functions_api.html#module-icclim._generated_api)
and an associated [ATBD](https://www.ecad.eu/documents/atbd.pdf).

`run_xclim.py` uses the [xclim](https://xclim.readthedocs.io/en/stable/) library
to calculate a series of climate indicators documented at the xclim
[climate indicators API](https://xclim.readthedocs.io/en/stable/indicators.html).

The major difference between the icclim and xclim indices is that the icclim indices
represent a set of widely used, rigidly defined metrics
(e.g. the RX5day index can't be modified to calculate the maximum rainfall over 4 days instead)
while the xclim indicators can be configured to set different thresholds, times of year, etc.

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

## Usage: `run_icclim.py`

Running the script at the command line with the `-h` option explains the user options,
including the very long list of indices that the script can calculate.

```bash
$ python run_icclim.py -h
```

#### Example 1: Simple indices

The most basic use of `run_icclim.py` requires passing the script 
the name of the index to calculate,
the name of the output file,
the name/s of the input file/s, and
the name of the variable to access from that file/s,
For example:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py TXx /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/climdex/txx/txx_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_year_197901-200112.nc --input_files /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/output/AUS-15/BOM/ECMWF-ERA5/evaluation/r1i1p1f1/BOM-BARPA-R/v1/day/tasmax/tasmax_AUS-15_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_day_*.nc --variable tasmax --verbose
```

In the example above,
the `--verbose` flag has also been invoked so that the program prints its progress to the screen.

#### Example 2: Sub-daily input data

All the icclim climate indices are calculated from daily data.
If you want to input sub-daily (e.g. hourly) data instead,
you need to use the `--time_agg` option to specify how to temporally aggregate the data.
For instance,

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py TNn /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/GLOBAL-gn/none/ECMWF-ERA5/evaluation/r1i1p1f1/none/none/climdex/tnn/tnn_AUS-gn_ECMWF-ERA5_evaluation_r1i1p1f1_year_195901-202112.nc --input_files /g/data/rt52/era5/single-levels/reanalysis/2t/*/*.nc --variable t2m --time_agg min --start_date 1959-01-01 --end_date 2021-12-31 --lon_bnds 111.975 156.275 --lat_bnds -44.525 -9.975 --hshift --verbose
```

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py R10mm /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-gn/none/ECMWF-ERA5/evaluation/r1i1p1f1/none/none/climdex/r10mm/r10mm_AUS-gn_ECMWF-ERA5_evaluation_r1i1p1f1_year_195901-202112.nc --input_files /g/data/rt52/era5/single-levels/reanalysis/mtpr/*/*.nc --variable mtpr --time_agg mean --start_date 1959-01-01 --end_date 2021-12-31 --lon_bnds 111.975 156.275 --lat_bnds -44.525 -9.975 --hshift --verbose
```

In both these examples, the `--hshift` option has been used to shift the time axis values back an hour
(which is required when working with ERA5 data).
The `--start_date`, `--end_date`, `--lat_bnds` and `--lon_bnds` options have also been used to subset the data.
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
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py DTR /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-r005/none/BOM-AGCD/historical/v1/none/none/climdex/dtr/dtr_AUS-r005_BOM-AGCD_historical_v1_year_191001-202112.nc  --input_files /g/data/xv83/agcd-csiro/tmax/daily/tmax_AGCD-CSIRO_r005_*.nc --variable tmax --input_files /g/data/xv83/agcd-csiro/tmin/daily/tmin_AGCD-CSIRO_r005_*.nc --variable tmin --start_date 1910-01-01 --end_date 2021-12-31 --verbose
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
For instance, in the example below the `R95pTOT` index requires calculation of the 95th percentile
along the time axis for the entire 1900-2021 period:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_icclim.py R95pTOT /g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/indices/AUS-r005/none/BOM-AGCD/historical/v1/none/none/climdex/r95ptot/r95ptot_AUS-r005_BOM-AGCD_historical_v1_year_190001-202112.nc --input_files /g/data/xv83/agcd-csiro/precip/daily/precip-total_AGCD-CSIRO_r005_19000101-20220405_daily_space-chunked.zarr --variable precip --start_date 1900-01-01 --end_date 2021-12-31 --verbose
```

Indices that involve calculations along the entire time axis are much more memory intensive.
The example above used about 115 GB of RAM on the Australian Research Environment at NCI and took about 15 minutes to run.

The icclim documentation has [extensive notes](https://icclim.readthedocs.io/en/stable/how_to/dask.html)
on how to improve performance for indices like this via data chunking and parallelisation using dask. 
The `run_icclim.py` script has a `--local_cluster` option that can be used
(in conjunction with the `--nworkers` and `--nthreads` options)
to launch and configure a local dask cluster.
We are still figuring out the optimal local cluster settings for different use cases
(and will add advice to this documentation if we find any useful approaches),
but in general the computation will be fastest if you simply allocate a large amount of memory to the job
(e.g. using the `largemem` job queue on NCI)
as opposed to fiddling around with a local dask cluster.
The only time allocating a large amount of memory possibly won't be sufficient
is when using the `--base_period` option, which seems to dramitically increase compute requirements.
We haven't been able to find dask cluster settings to overcome this problem,
so a `--nslices` option is available to slice up the input data (along the longitude axis)
and process each slice one-by-one before putting it all back together at the end. 


## Usage: `run_xclim.py`

Running the script at the command line with the `-h` option explains the user options,
including the list of indices that the script can calculate.

```bash
$ python run_xclim.py -h
```

#### Example 1: Simple indices

The most basic use of `run_xclim.py` requires passing the script 
the name of the index to calculate,
the name of the output file,
the name/s of the input file/s,
the name of the variable to access from that file/s,
a threshold value for the index, and
date bounds if analysing a restricted part of the year.
For example:

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_xclim.py frost_days frost_days.nc --input_files /g/data/wp00/data/QQ-CMIP5/ACCESS1-0/tasmin/rcp45/2036-2065/tasmin_AUS_ACCESS1-0_rcp45_r1i1p1_CSIRO-QQS-AGCD-1981-2010_day_wrt_1986-2005_2036-2065.nc --variable tasmin --thresh "0.1 degC" --date_bounds 08-01 10-15 --verbose
```

In the example above,
the `--verbose` flag has also been invoked so that the program prints its progress to the screen.


#### Example 2: Indices involving daily mean temperature

Many datasets archive daily minimum and maximum temperature, but not daily mean temperature.
For metrics that require daily mean temperature,
`run_xclim.py` will calculate the mean if you input tasmin and tasmax. e.g.

```
$ /g/data/xv83/dbi599/miniconda3/envs/icclim/bin/python run_xclim.py growing_degree_days gdd.nc --input_files /g/data/wp00/data/QQ-CMIP5/ACCESS1-0/tasmax/rcp45/2036-2065/tasmax_AUS_ACCESS1-0_rcp45_r1i1p1_CSIRO-QQS-AGCD-1981-2010_day_wrt_1986-2005_2036-2065.nc --variable tasmax --input_files /g/data/wp00/data/QQ-CMIP5/ACCESS1-0/tasmin/rcp45/2036-2065/tasmin_AUS_ACCESS1-0_rcp45_r1i1p1_CSIRO-QQS-AGCD-1981-2010_day_wrt_1986-2005_2036-2065.nc --variable tasmin --thresh "0 degC" --date_bounds 04-01 10-31 --verbose
```

The trick here was to use the `--input_files` and `--variable` options twice;
once for the tasmax data and once for the tasmin.

#### Common configurations

Late season heat risk / wheat heat risk
- Description: Number of days per year where the maximum temperature is > 32C between 1 Aug and 30 Nov
- Arguments: `tx_days_above` metric with `--thresh "32 DegC"` and `--date_bounds 08-01 11-30`

Heat risk fertility metric / sheep heat risk
- Description: Number of days per year where the maximum temperature is > 32C between 15 Jan and 15 Jun
- Arguments: `tx_days_above` metric with `--thresh "32 DegC"` and `--date_bounds 01-15 06-15`
