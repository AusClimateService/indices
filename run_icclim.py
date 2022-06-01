"""Command line program for calculating extremes indices."""

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
    else:
        ValueError(f'No metadata fixes defined for {dataset} {variable}')

    return ds


def main(args):
    """Run the program."""

    ds = xr.open_mfdataset(args.in_files)
    if args.dataset:
        ds = fix_metadata(ds, args.dataset, args.var_name)

    index = icclim.index(
        in_files=ds,
        index_name=args.index_name,
        var_name=args.var_name,
        slice_mode=args.slice_mode,
    )
    index.attrs['history'] = get_new_log()
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
    args = parser.parse_args()
    main(args)
