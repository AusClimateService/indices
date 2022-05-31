"""Command line program for calculating extremes indices."""

import argparse

import icclim
import dask.diagnostics


dask.diagnostics.ProgressBar().register()


def main(args):
    """Run the program."""

    icclim.index(
        in_files=args.in_files,
        index_name=args.index_name,
        var_name=args.var_name,
        slice_mode=args.slice_mode,
        out_file=args.out_file
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    valid_indices = [
        'cdd',
        'cwd',
        'prcptot',
        'r10mm',
        'r20mm',
        'r95ptot',
        'rx1day',
        'rx5day',
    ]              
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
    args = parser.parse_args()
    main(args)
