"""Command line program for calculating climate indices using xclim."""

import os
import argparse
import logging

import xclim as xc
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress

import fileio_utils


index_variables = {
    'cooling_degree_days': ['tas'],
    'frost_days': ['tasmin'],
    'growing_degree_days': ['tas'],
    'heat_wave_frequency': ['tasmin', 'tasmax'],
    'hot_spell_frequency': ['tasmax'],
    'hot_spell_max_length': ['tasmax'],
    'maximum_consecutive_dry_days': ['pr'],
    'tn_days_below': ['tasmin'],
    'tn_days_above': ['tasmin'],
}

index_func = {
    'cooling_degree_days': xc.indicators.atmos.cooling_degree_days,
    'frost_days': xc.indicators.atmos.frost_days,
    'growing_degree_days': xc.indicators.atmos.growing_degree_days,
    'heat_wave_frequency': xc.indicators.atmos.heat_wave_frequency,
    'hot_spell_frequency': xc.indicators.atmos.hot_spell_frequency,
    'hot_spell_max_length': xc.indicators.atmos.hot_spell_max_length,
    'maximum_consecutive_dry_days': xc.indicators.atmos.maximum_consecutive_dry_days,
    'tn_days_below': xc.indicators.atmos.tn_days_below,
    'tn_days_above': xc.indicators.atmos.tn_days_below,
}

# indices where thresh keyword argument is thresh_{var}
thresh_var_indices = ['hot_spell_frequency', 'hot_spell_max_length']


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

    da_dict = {}
    thresh_dict = {}
    ndatasets = len(args.variable)
    index_vars = index_variables[args.index_name]
    for dsnum in range(ndatasets):
        ds, cf_var = fileio_utils.read_data(
            args.input_files[dsnum],
            args.variable[dsnum],
            start_date=args.start_date,
            end_date=args.end_date,
            lat_bnds=args.lat_bnds,
            lon_bnds=args.lon_bnds,
#            sub_daily_agg=args.sub_daily_agg,
            hshift=args.hshift,
        )
        da_dict[cf_var] = ds[cf_var]
        if 'tas' in index_vars:
            thresh_dict['tas'] = args.thresh[0]
        else:
            thresh_dict[cf_var] = args.thresh[dsnum]

    if 'tas' in index_vars:
        if 'tas' not in da_dict:
            da_dict['tas'] = (da_dict['tasmax'] + da_dict['tasmin']) / 2.0
            da_dict['tas'].attrs = da_dict['tasmin'].attrs
            da_dict['tas'].attrs['long_name'] = da_dict['tas'].attrs['long_name'].replace('Minimum', 'Mean')
            da_dict['tas'].name = 'tas'

    kwargs = {'freq': args.freq}
    if args.date_bounds: 
        kwargs['date_bounds'] = args.date_bounds
    if len(index_vars) == 1:
        var = index_vars[0]
        kwargs[var] = da_dict[var]
        if args.index_name in thresh_var_indices:
            thresh_key = f'thresh_{var}'
        else:
            thresh_key = 'thresh'
        kwargs[thresh_key] = thresh_dict[var]
    elif len(index_vars) == 2:
        var1, var2 = index_vars
        kwargs[var1] = da_dict[var1]
        kwargs[var2] = da_dict[var2]
        kwargs[f'thresh_{var1}'] = thresh_dict[var1]
        kwargs[f'thresh_{var2}'] = thresh_dict[var2]  
    else:
        raise ValueError('Too many input variables')

    index = index_func[args.index_name](**kwargs)
    index = index.to_dataset()

    if args.local_cluster:
        index = index.persist()
        progress(index)

    if args.append_history:
        infile_log = {infiles[0], ds.attrs['history']}
    else:
        infile_log = None
    index = fileio_utils.fix_output_metadata(
        index, args.index_name, ds.attrs, infile_log, 'xclim'
    )
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
    arg_parser.add_argument(
        "--thresh",
        type=str,
        action='append',
        help='threshold'
    )
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
    fileio_utils.profiling_stats(rprof)
