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
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress
import cmdline_provenance as cmdprov


def profiling_stats(rprof):
    """Record profiling information"""

    max_memory = np.max([result.mem for result in rprof.results])
    max_cpus = np.max([result.cpu for result in rprof.results])

    logging.info(f'Peak memory usage: {max_memory}MB')
    logging.info(f'Peak CPU usage: {max_cpus}%')


def get_new_log(infile, history):
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(
        infile_logs = {infile: history},
        code_url=repo_url,
    )

    return new_log


def fix_metadata(ds, variable):
    """Ensure CF-compliance (which icclim requires)"""

    if variable in ['precip', 'pr']:
        standard_name = 'precipitation_flux'
        long_name = 'Precipitation'
    elif variable in ['tmax', 'tasmax']:
        standard_name = 'air_temperature',
        long_name = 'Daily Maximum Near-Surface Air Temperature'
    elif variable in ['tmin', 'tasmin']:
        standard_name = 'air_temperature',
        long_name = 'Daily Minimum Near-Surface Air Temperature',
    else:
        ValueError(f'No metadata fixes defined for {variable}')
    ds[variable].attrs['standard_name'] = standard_name
    ds[variable].attrs['long_name'] = long_name        

    units = ds[variable].attrs['units']
    if units in ['degrees_Celsius']:
        ds[variable].attrs['units'] = 'degC' 
    elif units in ['mm']:
        ds[variable].attrs['units'] = 'mm d-1'

    return ds


def subset_and_chunk(ds, var, index_name, time_period=None, lon_chunk_size=None):
    """Subset and chunk a dataset."""

    if time_period:
        start_date, end_date = time_period
        ds = ds.sel({'time': slice(start_date, end_date)})

    if index_name in ['r95ptot', 'r99ptot', 'wsdi']:
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


def read_data(infiles, variable_name):
    """Read the input data file/s."""

    if len(infiles) == 1:
        ds = xr.open_dataset(infiles[0], chunks='auto')
    else:
        ds = xr.open_mfdataset(infiles, chunks='auto')
    ds = fix_metadata(ds, variable_name)

    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    return ds


def main(args):
    """Run the program."""

    assert not os.path.isfile(args.output_file), \
        f'Output file {args.output_file} already exists. Delete before proceeding.'

    if args.local_cluster:
        assert args.dask_dir, "Must provide --dask_dir for local cluster"
        dask.config.set(temporary_directory=args.dask_dir)
        cluster = LocalCluster(
            memory_limit='auto',
            n_workers=args.nworkers,
            threads_per_worker=args.nthreads,
        )
        client = Client(cluster)
        print("Watch progress at http://localhost:8787/status")
    else:
        dask.diagnostics.ProgressBar().register()

    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level)

    ds = read_data(args.input_files, args.var_name)
    ds = subset_and_chunk(
        ds,
        args.var_name,
        args.index_name,
        time_period=args.time_period,
    )

    if args.base_period:
        start_date = parser.parse(args.base_period[0])
        end_date = parser.parse(args.base_period[1])
        base_period = [start_date, end_date]
    else:
        base_period = None

    index = icclim.index(
        in_files=ds,
        index_name=args.index_name,
        var_name=args.var_name,
        slice_mode=args.slice_mode,
        base_period_time_range=base_period,
        logs_verbosity='HIGH',
    )

    if args.local_cluster:
        index = index.persist()
        progress(index)
    index.attrs['history'] = get_new_log(args.input_files[0], ds.attrs['history'])

    if args.drop_time_bounds:
        index = index.drop('time_bounds').drop('bounds')
        del index['time'].attrs['bounds']
    index.to_netcdf(args.output_file)


if __name__ == '__main__':
    valid_indices = [index.short_name.lower() for index in EcadIndexRegistry.values()]
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    arg_parser.add_argument("input_files", type=str, nargs='*', help="input files")
    arg_parser.add_argument("var_name", type=str, help="variable name")
    arg_parser.add_argument("index_name", type=str, choices=valid_indices, help="index name")         
    arg_parser.add_argument("output_file", type=str, help="output file name")
    arg_parser.add_argument(
        "--time_period",
        type=str,
        nargs=2,
        default=None,
        help='Time period in YYYY-MM-DD format',
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
        "--drop_time_bounds",
        action='store_true',
        default=False,
        help='Drop the time bounds from output file',
    )
    arg_parser.add_argument(
        "--memory_vis",
        action="store_true",
        default=False,
        help='Visualise memory and CPU usage (creates profile.html)',
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
        "--dask_dir",
        type=str,
        default=None,
        help='Directory where dask worker space files can be written. Required for local dask cluster.',
    )
    args = arg_parser.parse_args()

    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    if args.memory_vis:
        rprof.visualize(filename='profile.html')
    profiling_stats(rprof)
