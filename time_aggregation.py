"""Command line program for temporal aggregation."""

import os
import argparse
import logging

import numpy as np
import xarray as xr
import xcdat as xc

import fileio_utils



def temporal_aggregation(
    ds,
    target_freq,
    input_freq,
    agg_method,
    variables,
    season=None,
    reset_times=False,
    complete=False,
    agg_dates=False,
    time_dim="time",
):
    """Temporal aggregation of data.
    Parameters
    ----------
    ds : xarray Dataset
    target_freq : {'A-DEC', 'Q-NOV', 'M', 'A-NOV', 'A-AUG'}
        Target frequency for the resampling
    agg_method : {'mean', 'min', 'max', 'sum'}
        Aggregation method
    variables : list
        Variables in the dataset
    input_freq : {'D', 'M', 'Q', 'A'}
        Temporal frequency of input data (daily, monthly or annual)
    season : {'DJF', 'MAM', 'JJA', 'SON'}, optional
        Select a single season after Q-NOV resampling
    reset_times : bool, default False
        Shift time values after resampling so months match initial date
    agg_dates : bool, default False
        Record the date of each time aggregated event (e.g. annual max)
    complete : bool, default False
        Keep only complete time units (e.g. complete years or months)
    time_dim: str, default 'time'
        Name of the time dimension in ds
    Returns
    -------
    ds : xarray Dataset
    Notes
    -----
    A-DEC = annual, with date label being last day of year
    M = monthly, with date label being last day of month
    Q-NOV = DJF, MAM, JJA, SON, with date label being last day of season
    A-NOV = annual Dec-Nov, date label being last day of the year
    A-AUG = annual Sep-Aug, date label being last day of the year
    """

    assert target_freq in ["A-DEC", "M", "Q-NOV", "A-NOV", "A-AUG"]
    assert input_freq in ["D", "M", "Q", "A"]

    if time_dim not in ds.dims:
        ds = array_handling.reindex_forecast(ds)
        reindexed = True
    else:
        reindexed = False

    start_time = ds[time_dim].values[0]
    counts = ds[variables[0]].resample(time=target_freq).count(dim=time_dim)

    if input_freq == target_freq[0]:
        pass
    elif agg_method in ["max", "min"]:
        if agg_dates:
            agg_dates_var = get_agg_dates(
                ds, variables[0], target_freq, agg_method, time_dim=time_dim
            )
        if agg_method == "max":
            ds = ds.resample(time=target_freq).max(dim=time_dim, keep_attrs=True)
        else:
            ds = ds.resample(time=target_freq).min(dim=time_dim, keep_attrs=True)
        if agg_dates:
            ds = ds.assign(event_time=(ds[variables[0]].dims, agg_dates_var))
    elif agg_method == "sum":
        ds = ds.resample(time=target_freq).sum(dim=time_dim, keep_attrs=True)
        for var in variables:
            ds[var].attrs["units"] = _update_rate(ds[var], input_freq, target_freq)
    elif agg_method == "mean":
        if input_freq == "D":
            ds = ds.resample(time=target_freq).mean(dim=time_dim, keep_attrs=True)
        elif input_freq == "M":
            ds = _monthly_downsample_mean(ds, target_freq, variables, time_dim=time_dim)
        else:
            raise ValueError(f"Unsupported input time frequency: {input_freq}")
    else:
        raise ValueError(f"Unsupported temporal aggregation method: {agg_method}")

    if season:
        assert target_freq == "Q-NOV"
        final_month = {"DJF": 2, "MAM": 5, "JJA": 8, "SON": 11}
        season_month = final_month[season]
        ds = select_month(ds, season_month, time_dim=time_dim)

    if reset_times:
        diff = ds[time_dim].values[0] - start_time
        ds[time_dim] = ds[time_dim] - diff
        assert ds[time_dim].values[0] == start_time

    if complete:
        for var in variables:
            ds[var] = _crop_to_complete_time_periods(
                ds[var], counts, input_freq, target_freq
            )

    if reindexed:
        ds = ds.compute()
        ds = array_handling.time_to_lead(ds, target_freq[0])

    return ds


def _monthly_downsample_mean(ds, target_freq, variables, time_dim="time"):
    """Downsample monthly data (e.g. to seasonal or annual).

    Accounts for the different number of days in each month.
    """

    days_in_month = ds[time_dim].dt.days_in_month
    weighted_mean = (ds * days_in_month).resample(time=target_freq).sum(
        dim=time_dim, keep_attrs=True
    ) / days_in_month.resample(time=target_freq).sum(dim=time_dim)
    weighted_mean.attrs = ds.attrs
    for var in variables:
        weighted_mean[var].attrs = ds[var].attrs

    return weighted_mean





def main(args):
    """Run the program."""

    ds = xc.open_dataset(args.infiles)
    





    if args.append_history:
        infile_log = {infiles[0], ds.attrs['history']}
    else:
        infile_log = None

    index.to_netcdf(args.output_file)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    arg_parser.add_argument("input_files", type=str, nargs='*', help="input files")
    arg_parser.add_argument("variable", type=str, help="variable to process from input files")
    arg_parser.add_argument("output_file", type=str, help="output file name")
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
    args = arg_parser.parse_args()
    main(args)

