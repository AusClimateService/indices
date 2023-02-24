"""Command line program for calculating climate indices using xclim."""

import os
import argparse
import logging

import numpy as np
import xclim as xc
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress

import fileio_utils


index_variables = {
    'cooling_degree_days': ['tas',],
    'frost_days': ['tasmin',],
    'growing_degree_days': ['tas',],
}

index_func = {
    'cooling_degree_days': xc.indicators.atmos.cooling_degree_days,
    'frost_days': xc.indicators.atmos.frost_days,
    'growing_degree_days': xc.indicators.atmos.growing_degree_days,
}


def profiling_stats(rprof):
    """Record profiling information"""

    max_memory = np.max([result.mem for result in rprof.results])
    max_cpus = np.max([result.cpu for result in rprof.results])

    logging.info(f'Peak memory usage: {max_memory}MB')
    logging.info(f'Peak CPU usage: {max_cpus}%')


def fix_output_metadata(index_ds, index_name, infile_log, input_global_attrs):
    """Make edits to output xclim metadata"""

    new_global_attrs = input_global_attrs
    new_global_attrs['xclim_version'] = xc.__version__
    new_global_attrs['history'] = fileio_utils.get_new_log(infile_log=infile_log)
    index_ds.attrs = new_global_attrs
    
    del index_ds[index_name].attrs['history']
    del index_ds[index_name].attrs['cell_methods']
    
    return index_ds


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

    ds, cf_var = fileio_utils.read_data(
        args.input_files[0],
        args.variable[0],
        start_date=args.start_date,
        end_date=args.end_date,
        lat_bnds=args.lat_bnds,
        lon_bnds=args.lon_bnds,
#        sub_daily_agg=args.sub_daily_agg,
        hshift=args.hshift,
    )
    
    index = index_func[args.index_name](
        ds[cf_var], thresh=args.thresh, freq=args.freq, date_bounds=args.date_bounds
    )
    index = index.to_dataset()

    if args.local_cluster:
        index = index.persist()
        progress(index)

    if args.append_history:
        infile_log = {infiles[0], ds.attrs['history']}
    else:
        infile_log = None
    index = fix_output_metadata(index, args.index_name, infile_log, ds.attrs)
    index.to_netcdf(args.output_file)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    arg_parser.add_argument(
        "index_name", type=str, choices=list(index_func.keys()), help="name of climate index"
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
    arg_parser.add_argument("--thresh", type=str, help="threshold")
    arg_parser.add_argument(
        "--date_bounds",
        type=str,
        nargs=2,
        default=None,
        help='Bounds for the time of year of interest in MM-DD format',
    )
    arg_parser.add_argument(
        "--freq",
        type=str,
        default='YS',
        help='Sampling frequency for index calculation [default=YS]',
    )
    arg_parser.add_argument(
        "--hshift",
        action='store_true',
        default=False,
        help='Shfit time axis values back one hour (required for ERA5 data)',
    )
    arg_parser.add_argument(
        "--append_history",
        action='store_true',
        default=False,
        help='Append new history file attribute to history from input file/s',
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
        help='Latitude bounds: (south_bound, north_bound)',
    )
    arg_parser.add_argument(
        "--lon_bnds",
        type=float,
        nargs=2,
        default=None,
        help='Longitude bounds: (west_bound, east_bound)',
    )
    arg_parser.add_argument(
        "--sub_daily_agg",
        type=str,
        action='append',
        choices=('min', 'mean', 'max'),
        default=None,
        help="temporal aggregation to apply to sub-daily input files (used to convert hourly to daily)",
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
    args = arg_parser.parse_args()

    assert not os.path.isfile(args.output_file), \
        f'Output file {args.output_file} already exists. Delete before proceeding.'

    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    profiling_stats(rprof)
