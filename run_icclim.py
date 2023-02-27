"""Command line program for calculating climate indices using icclim."""

import os
import argparse
import logging
from dateutil import parser

import icclim
from icclim.ecad.ecad_indices import EcadIndexRegistry
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress

import fileio_utils


valid_indices = {
    index.short_name: index.short_name for index in EcadIndexRegistry.values()
}

bivariate_indices = [
    'DTR',
    'ETR',
    'vDTR',
    'CD',
    'CW',
    'WD',
    'WW'
]
no_time_chunk_indices = [
    'WSDI',
    'TG90p',
    'TN90p',
    'TX90p',
    'TG10p',
    'TN10p',
    'TX10p',
    'CSDI',
    'R95p',
    'R95pTOT',
    'R99p',
    'R99pTOT',
    'CD',
    'CW',
    'WD',
    'WW',
]
    

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
        sub_daily_agg = args.sub_daily_agg[dsnum] if args.sub_daily_agg else None
        ds, cf_var = fileio_utils.read_data(
            infiles,
            var,
            start_date=args.start_date,
            end_date=args.end_date,
            lat_bnds=args.lat_bnds,
            lon_bnds=args.lon_bnds,
            sub_daily_agg=sub_daily_agg,
            hshift=args.hshift,
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

    if args.append_history:
        infile_log = {infiles[0], ds.attrs['history']}
    else:
        infile_log = None
    index = fileio_utils.fix_output_metadata(
        index,
        args.index_name,
        ds.attrs,
        infile_log,
        'icclim',
        drop_time_bounds=args.drop_time_bounds
    )
    index[args.index_name] = index[args.index_name].transpose('time', 'lat', 'lon', missing_dims='warn')
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
        "--sub_daily_agg",
        type=str,
        action='append',
        choices=('min', 'mean', 'max'),
        default=None,
        help="temporal aggregation to apply to sub-daily input files (used to convert hourly to daily)",
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
    fileio_utils.profiling_stats(rprof)
