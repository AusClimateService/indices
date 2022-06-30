"""Command line program for calculating extremes indices."""
import pdb
import argparse
import logging
from dateutil import parser

import git
import numpy as np
import xarray as xr
import icclim
from icclim.models.ecad_indices import EcadIndex
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


def fix_metadata(ds, dataset, variable):
    """Apply dataset- and variable-specific metdata fixes.

    icclim (using xclim under the hood) does CF-compliance checks
      that some datasets fail
    """

    if (dataset == 'AGCD') and (variable == 'precip'):
        ds['precip'].attrs = {
            'standard_name': 'precipitation_flux',
            'long_name': 'Precipitation',
            'units': 'mm d-1',
        }
    elif (dataset == 'AGCD') and (variable == 'tmax'):
        ds['tmax'].attrs = {
            'standard_name': 'air_temperature',
            'long_name': 'Daily Maximum Near-Surface Air Temperature',
            'units': 'degC',
        }
    elif (dataset == 'AGCD') and (variable == 'tmin'):
        ds['tmin'].attrs = {
            'standard_name': 'air_temperature',
            'long_name': 'Daily Minimum Near-Surface Air Temperature',
            'units': 'degC',
        }
    else:
        ValueError(f'No metadata fixes defined for {dataset} {variable}')

    return ds


def main(args):
    """Run the program."""

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level)

    if args.local_cluster:
        dask.config.set(temporary_directory='/g/data/xv83/dbi599/')
        cluster = LocalCluster(n_workers=args.nworkers)
        client = Client(cluster)
        print("Watch progress at http://localhost:8787/status")
    else:
        dask.diagnostics.ProgressBar().register()

    infiles = args.in_files[0] if len(args.in_files) == 1 else args.in_files
    ds = xr.open_mfdataset(infiles)
    if args.dataset:
        ds = fix_metadata(ds, args.dataset, args.var_name)
    if args.index_name in ['r95ptot', 'r99ptot']:
        ds = ds.chunk({'time': -1, 'lon': 1})
    try:
        ds = ds.drop('height')
    except ValueError:
        pass
    if args.time_period:
        start_date, end_date = args.time_period
        ds = ds.sel({'time': slice(start_date, end_date)})
    logging.info(f'Array size: {ds[args.var_name].shape}')
    logging.info(f'Chunk size: {ds[args.var_name].chunksizes}')

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
    )
    index.attrs['history'] = get_new_log(args.in_files[0], ds.attrs['history'])

    if args.drop_time_bounds:
        index = index.drop('time_bounds').drop('bounds')
        del index['time'].attrs['bounds']
    index.to_netcdf(args.out_file)

if __name__ == '__main__':

    valid_indices = []
    for index in EcadIndex:
        if not index.group.name == 'COMPOUND':
            valid_indices.append(index.short_name.lower())

    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    arg_parser.add_argument("in_files", type=str, nargs='*', help="input files")
    arg_parser.add_argument("index_name", type=str, choices=valid_indices, help="index name")         
    arg_parser.add_argument("var_name", type=str, help="variable name")
    arg_parser.add_argument("out_file", type=str, help="output file name")
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
        help='Sampling frequency for index calculation',
    )
    arg_parser.add_argument(
        "--dataset",
        type=str,
        choices=['AGCD'],
        default=None,
        help='Apply dataset and variable specific metadata fixes for CF compliance',
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
        help='Set logging level to DEBUG',
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
        help='Number of workers for cluster [default lets dask decide]',
    )
    args = arg_parser.parse_args()
    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    if args.memory_vis:
        rprof.visualize(filename='profile.html')
    profiling_stats(rprof)
