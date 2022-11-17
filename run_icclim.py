"""Command line program for calculating extremes indices."""

import os
import argparse
import logging
from dateutil import parser

import git
import numpy as np
import xarray as xr
import icclim
from icclim.ecad.ecad_indices import EcadIndexRegistry
from clisops.core.subset import subset_bbox
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress
import cmdline_provenance as cmdprov


valid_indices = {
    index.short_name.lower(): index.short_name for index in EcadIndexRegistry.values()
}
bivariate_indices = [
    'dtr',
    'etr',
    'vdtr',
    'cd',
    'cw',
    'wd',
    'ww'
]
no_time_chunk_indices = [
    'wsdi',
    'tg90p',
    'tn90p',
    'tx90p',
    'tg10p',
    'tn10p',
    'tx10p',
    'csdi',
    'r75p',
    'p75ptot',
    'r95p',
    'r95ptot',
    'r99p',
    'r99ptot',
    'cd',
    'cw',
    'wd',
    'ww',
]
standard_names = {
    'pr': 'precipitation_flux',
    'tas': 'air_temperature',
    'tasmin': 'air_temperature',
    'tasmax': 'air_temperature',
}
long_names = {
    'pr': 'Precipitation',
    'tas': 'Near-Surface Air Temperature',
    'tasmin': 'Daily Minimum Near-Surface Air Temperature',
    'tasmax': 'Daily Maximum Near-Surface Air Temperature',
}
    

def profiling_stats(rprof):
    """Record profiling information"""

    max_memory = np.max([result.mem for result in rprof.results])
    max_cpus = np.max([result.cpu for result in rprof.results])

    logging.info(f'Peak memory usage: {max_memory}MB')
    logging.info(f'Peak CPU usage: {max_cpus}%')


def get_new_log():
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(code_url=repo_url)

    return new_log


def fix_input_metadata(ds, variable, time_agg):
    """Ensure CF-compliance of input data file (which icclim requires)"""

    if 'latitude' in ds.dims:
        ds = ds.rename({'latitude': 'lat'})
    if 'longitude' in ds.dims:
        ds = ds.rename({'longitude': 'lon'})
    
    if variable in ['pr', 'precip', 'mtpr', 'tp']:
        cf_var = 'pr'
    elif variable in ['tasmax', 'tmax', 'mx2t']:
        cf_var = 'tasmax'
    elif variable in ['tasmin', 'tmin', 'mn2t']:
        cf_var = 'tasmin'
    elif variable in ['tas', 't2m', '2t']:
        if time_agg == 'max':
            cf_var = 'tasmax'
        elif time_agg == 'min':
            cf_var = 'tasmin'
        else:
            cf_var = 'tas'
    else:
        ValueError(f'No metadata fixes defined for {variable}')
    ds = ds.rename({variable: cf_var})
    
    ds[cf_var].attrs['standard_name'] = standard_names[cf_var]
    ds[cf_var].attrs['long_name'] = long_names[cf_var]        

    units = ds[cf_var].attrs['units']
    if units in ['degrees_Celsius']:
        ds[cf_var].attrs['units'] = 'degC' 
    elif units in ['mm']:
        ds[cf_var].attrs['units'] = 'mm d-1'

    return ds, cf_var


def fix_output_metadata(index_ds, index_name_lower, drop_time_bounds=False):
    """Make edits to output metadata"""
    
    index_ds.attrs['history'] = get_new_log()
    
    index_name_upper = valid_indices[index_name_lower]
    del index_ds[index_name_upper].attrs['history']
    del index_ds[index_name_upper].attrs['cell_methods']
    
    if drop_time_bounds:
        index_ds = index_ds.drop('time_bounds').drop('bounds')
        del index_ds['time'].attrs['bounds']
    
    return index_ds


def chunk_data(ds, var, index_name):
    """Chunk a dataset."""

    if index_name in no_time_chunk_indices:
        dims = ds[var].coords.dims
        assert 'time' in dims
        chunks = {'time': -1}
        for dim in dims:
            if not dim == 'time':
                chunks[dim] = 'auto'
        ds = ds.chunk(chunks)

    logging.info(f'Array size: {ds[var].shape}')
    logging.info(f'Chunk size: {ds[var].chunksizes}')

    return ds


def read_data(infiles, variable_name, start_date=None, end_date=None, lat_bnds=None, lon_bnds=None, time_agg=None):
    """Read the input data file/s."""

    if len(infiles) == 1:
        ds = xr.open_dataset(infiles[0], chunks='auto', mask_and_scale=True)
    else:
        ds = xr.open_mfdataset(infiles, chunks='auto', mask_and_scale=True)

    subset_kwargs = {'start_date': start_date, 'end_date': end_date, 'lat_bnds': lat_bnds}
    if lon_bnds:
        subset_kwargs['lon_bnds'] = lon_bnds
    ds = subset_bbox(ds, **subset_kwargs)
    
    time_freq = xr.infer_freq(ds['time'])
    if time_agg and (time_freq != 'D'):
        logging.info(f'resampling from {time_freq} to daily {time_agg}')
        if time_agg == 'mean':
            ds = ds.resample(time='1D').mean(dim='time')
        elif time_agg == 'min':
            ds = ds.resample(time='1D').min(dim='time')
        elif time_agg == 'max':
            ds = ds.resample(time='1D').max(dim='time')
        time_freq = xr.infer_freq(ds['time'])
    assert time_freq == 'D', "Data must be daily timescale"

    ds, cf_var = fix_input_metadata(ds, variable_name, time_agg)
    
    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    try:
        ds = ds.drop('time_bnds')
    except ValueError:
        pass

    return ds, cf_var


def main(args):
    """Run the program."""

    if args.local_cluster:
        assert args.dask_dir, "Must provide --dask_dir for local cluster"
        dask.config.set(temporary_directory=args.dask_dir)
        cluster = LocalCluster(
            memory_limit=args.memory_limit,
            n_workers=args.nworkers,
            threads_per_worker=args.nthreads,
        )
        client = Client(cluster)
        print("Watch progress at http://localhost:8787/status")
    else:
        dask.diagnostics.ProgressBar().register()

    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level)

    if args.base_period:
        start_date = parser.parse(args.base_period[0])
        end_date = parser.parse(args.base_period[1])
        base_period = [start_date, end_date]
    else:
        base_period = None

    datasets = []
    variables = []
    ndatasets = len(args.variable)
    for dsnum in range(ndatasets):
        infiles = args.input_files[dsnum]
        var = args.variable[dsnum]
        time_agg = args.time_agg[dsnum] if args.time_agg else None
        ds, cf_var = read_data(
            infiles,
            var,
            start_date=args.start_date,
            end_date=args.end_date,
            lat_bnds=args.lat_bnds,
            lon_bnds=args.lon_bnds,
            time_agg=time_agg,
        )
        ds = chunk_data(ds, cf_var, args.index_name)
        datasets.append(ds)
        variables.append(cf_var)

    index = icclim.index(
        in_files=datasets,
        index_name=args.index_name,
        var_name=variables,
        slice_mode=args.slice_mode,
        base_period_time_range=base_period,
        logs_verbosity='HIGH',
    )

    if args.local_cluster:
        index = index.persist()
        progress(index)

    index = fix_output_metadata(
        index, args.index_name, drop_time_bounds=args.drop_time_bounds
    )
    index.to_netcdf(args.output_file)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    arg_parser.add_argument(
        "index_name", type=str, choices=list(valid_indices.keys()), help="index name"
    )         
    arg_parser.add_argument("output_file", type=str, help="output file name")
    arg_parser.add_argument(
        "--input_files",
        type=str,
        nargs='*',
        action='append',
        help="input files for a particular variable",
    )
    arg_parser.add_argument(
        "--variable",
        type=str,
        action='append',
        help="variable to process from input files",
    )
    arg_parser.add_argument(
        "--time_agg",
        type=str,
        action='append',
        choices=('min', 'mean', 'max'),
        default=None,
        help="temporal aggregation to apply to input files (used to convert hourly to daily)",
    )
    arg_parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help='Start date in YYYY, YYYY-MM or YYYY-MM-DD format',
    )
    arg_parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help='Start date in YYYY, YYYY-MM or YYYY-MM-DD format',
    )
    arg_parser.add_argument(
        "--lat_bnds",
        type=float,
        nargs=2,
        default=None,
        help='Latitude bounds',
    )
    arg_parser.add_argument(
        "--lon_bnds",
        type=float,
        nargs=2,
        default=None,
        help='Longitude bounds',
    )
    arg_parser.add_argument(
        "--base_period",
        type=str,
        nargs=2,
        default=None,
        help='Base period (for percentile calculations) in YYYY-MM-DD format',
    )
    arg_parser.add_argument(
        "--slice_mode",
        type=str,
        choices=['year', 'month', 'DJF', 'MAM', 'JJA', 'SON', 'ONDJFM', 'AMJJAS'],
        default='year',
        help='Sampling frequency for index calculation [default=year]',
    )
    arg_parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help='Set logging level to INFO',
    )
    arg_parser.add_argument(
        "--local_cluster",
        action="store_true",
        default=False,
        help='Use a local dask cluster',
    )
    arg_parser.add_argument(
        "--nworkers",
        type=int,
        default=None,
        help='Number of workers for local dask cluster',
    )
    arg_parser.add_argument(
        "--nthreads",
        type=int,
        default=None,
        help='Number of threads per worker for local dask cluster',
    )
    arg_parser.add_argument(
        "--memory_limit",
        type=str,
        default='auto',
        help='Memory limit for local dask cluster',
    )
    arg_parser.add_argument(
        "--dask_dir",
        type=str,
        default=None,
        help='Directory where dask worker space files can be written. Required for local dask cluster.',
    )
    arg_parser.add_argument(
        "--drop_time_bounds",
        action='store_true',
        default=False,
        help='Drop the time bounds from output file',
    )
    args = arg_parser.parse_args()

    assert not os.path.isfile(args.output_file), \
        f'Output file {args.output_file} already exists. Delete before proceeding.'

    if args.index_name in bivariate_indices:
        assert len(args.variable) == 2, \
            f'{args.index_name} requires two variables' 
        assert len(args.input_files) == 2, \
            f'{args.index_name} requires two sets of input file/s (one for each variable)'
    else:
        assert len(args.variable) == 1, \
            f'{args.index_name} requires one variable' 
        assert len(args.input_files) == 1, \
            f'{args.index_name} requires one set of input file/s'

    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    profiling_stats(rprof)
