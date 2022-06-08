"""Command line program for calculating extremes indices."""
import pdb
import argparse

import git
import xarray as xr
import icclim
from icclim.models.ecad_indices import EcadIndex
import dask.diagnostics
import cmdline_provenance as cmdprov


dask.diagnostics.ProgressBar().register()


def get_new_log():
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(code_url=repo_url)

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

    ds = xr.open_mfdataset(args.in_files)
    if args.dataset:
        ds = fix_metadata(ds, args.dataset, args.var_name)
    if args.index_name in ['r95ptot', 'r99ptot']:
        ds = ds.chunk({'time': -1, 'lon': 1})
    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    index = icclim.index(
        in_files=ds,
        index_name=args.index_name,
        var_name=args.var_name,
        slice_mode=args.slice_mode,
    )
    index.attrs['history'] = get_new_log()

    if args.drop_time_bounds:
        index = index.drop('time_bounds').drop('bounds')
        del index['time'].attrs['bounds']
    index.to_netcdf(args.out_file)

if __name__ == '__main__':

    valid_indices = []
    for index in EcadIndex:
        if not index.group.name == 'COMPOUND':
            valid_indices.append(index.short_name.lower())

    parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    parser.add_argument("in_files", type=str, nargs='*', help="input files")
    parser.add_argument("index_name", type=str, choices=valid_indices, help="index name")         
    parser.add_argument("var_name", type=str, help="variable name")
    parser.add_argument("out_file", type=str, help="output file name")
    parser.add_argument(
        "--slice_mode",
        type=str,
        choices=['year', 'month', 'DJF', 'MAM', 'JJA', 'SON', 'ONDJFM', 'AMJJAS'],
        default='year',
        help='Sampling frequency for index calculation',
    )
    parser.add_argument(
        "--dataset",
        type=str,
        choices=['AGCD'],
        default=None,
        help='Apply dataset and variable specific metadata fixes for CF compliance',
    )
    parser.add_argument(
        "--drop_time_bounds",
        action='store_true',
        default=False,
        help='Drop the time bounds from output file',
    ) 
    args = parser.parse_args()
    main(args)
