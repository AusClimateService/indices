"""Command line program for calculating climate indices using xclim."""

import os
import argparse
import logging

import xarray as xr
import xclim as xc
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress

import utils_fileio


#limit program to indices we've used/tested
valid_indices = [
    'cooling_degree_days',
    'frost_days',
    'growing_degree_days',
    'heat_wave_frequency',
    'hot_spell_frequency',
    'hot_spell_max_length',
    'maximum_consecutive_dry_days',
    'tn_days_below',
    'tn_days_above',
]


def get_xclim_params(args_dict):
    """Get the parameters and variables required by an xclim indicator function.

    Parameters
    ----------
    args_dict : dict
        Input arguments from the command line

    Returns
    -------
    index_func : xclim function
        Function for calculating the index
    index_params : dict
        Parameters to pass to index_func
    index_vars : list
        Variables required by index_func
    """
                
    index_name = args_dict['index_name']
    index_func = xc.core.indicator.registry[index_name.upper()].get_instance()
    index_params = {}
    index_vars = []
    thresh_names = []
    for name, param in index_func.parameters.items():
        if name in ['ds', 'indexer']:
            continue
        elif 'thresh' in name:
            thresh_names.append(name)
        elif param['kind'] == xc.core.utils.InputKind.VARIABLE:
            index_vars.append(name)
        elif name in args_dict:
            index_params[name] = args_dict[name]
            mstr, *ustr = str(param['default']).split(" ", maxsplit=1)
            if ustr:
                unit = xc.core.units.units2pint(ustr[0])
                index_params[name] = "{} {}".format(index_params[name], str(unit))     
        else:
            index_params[name] = param['default']

    #Some indicators have a 'thresh' argument and others have thresh_{variable}
    thresh_dict = {}
    if 'thresh' in args_dict:
        input_thresh = args_dict['thresh']
        if len(input_thresh) == 1:
            thresh_dict['thresh'] = input_thresh[0]
        for position, var in enumerate(index_vars):
            thresh_dict[f'thresh_{var}'] = input_thresh[position]
    for thresh_name in thresh_names:
        index_params[thresh_name] = thresh_dict[thresh_name]

    if 'date_bounds' in args_dict: 
        index_params['date_bounds'] = args_dict['date_bounds']

    return index_func, index_params, index_vars
                

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

    index_func, index_params, index_vars = get_xclim_params(vars(args))

    ds_list = []
    ndatasets = len(args.variable)
    for dsnum in range(ndatasets):
        ds, cf_var = utils_fileio.read_data(
            args.input_files[dsnum],
            args.variable[dsnum],
            start_date=args.start_date,
            end_date=args.end_date,
            lat_bnds=args.lat_bnds,
            lon_bnds=args.lon_bnds,
            sub_daily_agg=args.sub_daily_agg,
            hshift=args.hshift,
        )
        ds_list.append(ds)
    ds = xr.merge(ds_list)

    if 'tas' in index_vars and 'tas' not in ds:
        ds['tas'] = (ds['tasmax'] + ds['tasmin']) / 2.0
        ds['tas'].attrs = ds['tasmin'].attrs
        ds['tas'].attrs['long_name'] = ds['tas'].attrs['long_name'].replace('Minimum', 'Mean')

    logging.info("Running xclim indicator {} with parameters {}".format(args.index_name, index_params))

    index = index_func(ds=ds, **index_params)
    index = index.to_dataset()

    if args.local_cluster:
        index = index.persist()
        progress(index)

    if args.append_history:
        infile_log = {infiles[0], ds.attrs['history']}
    else:
        infile_log = None
    index = utils_fileio.fix_output_metadata(
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
        "index_name", type=str, choices=valid_indices, help="name of climate index"
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
    utils_fileio.profiling_stats(rprof)
