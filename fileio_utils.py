"""Utilities for file I/O and metadata handling"""

import logging

import git
import xarray as xr
import cmdline_provenance as cmdprov

import spatial_utils


def get_new_log(infile_log=None):
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(
        infile_logs=infile_log,
        code_url=repo_url
    )

    return new_log


def fix_input_metadata(ds, variable, sub_daily_agg):
    """Ensure CF-compliance of input data file (which icclim requires)"""

    if 'latitude' in ds.dims:
        ds = ds.rename({'latitude': 'lat'})
    if 'longitude' in ds.dims:
        ds = ds.rename({'longitude': 'lon'})
    
    if variable in ['pr', 'precip', 'mtpr', 'tp']:
        cf_var = 'pr'
    elif variable in ['tasmax', 'tmax', 'mx2t']:
        cf_var = 'tasmax'
    elif variable in ['tasmin', 'tmin', 'mn2t']:
        cf_var = 'tasmin'
    elif variable in ['tas', 't2m', '2t', 'tasmean']:
        if sub_daily_agg == 'max':
            cf_var = 'tasmax'
        elif sub_daily_agg == 'min':
            cf_var = 'tasmin'
        else:
            cf_var = 'tas'
    else:
        ValueError(f'No metadata fixes defined for {variable}')
    ds = ds.rename({variable: cf_var})
    
    standard_names = {
        'pr': 'precipitation_flux',
        'tas': 'air_temperature',
        'tasmin': 'air_temperature',
        'tasmax': 'air_temperature',
    }
    long_names = {
        'pr': 'Precipitation',
        'tas': 'Near-Surface Air Temperature',
        'tasmin': 'Daily Minimum Near-Surface Air Temperature',
        'tasmax': 'Daily Maximum Near-Surface Air Temperature',
    }
    ds[cf_var].attrs['standard_name'] = standard_names[cf_var]
    ds[cf_var].attrs['long_name'] = long_names[cf_var]        

    units = ds[cf_var].attrs['units']
    if units in ['degrees_Celsius']:
        ds[cf_var].attrs['units'] = 'degC' 
    elif units in ['mm']:
        ds[cf_var].attrs['units'] = 'mm d-1'

    return ds, cf_var


def read_data(
    infiles,
    variable_name,
    start_date=None,
    end_date=None,
    lat_bnds=None,
    lon_bnds=None,
    sub_daily_agg=None,
    hshift=False
):
    """Read the input data file/s."""

    if len(infiles) == 1:
        try:
            ds = xr.open_dataset(infiles[0], chunks='auto', mask_and_scale=True)
        except NotImplementedError:
            ds = xr.open_dataset(infiles[0], mask_and_scale=True)
    else:
        try:
            ds = xr.open_mfdataset(infiles, chunks='auto', mask_and_scale=True)
        except NotImplementedError:
            ds = xr.open_mfdataset(infiles, mask_and_scale=True)

    if hshift:
        ds['time'] = ds['time'] - np.timedelta64(1, 'h')

    if lat_bnds:
        ds = spatial_utils.subset_lat(ds, lat_bnds)
    if lon_bnds:
        ds = spatial_utils.subset_lon(ds, lon_bnds)
    if start_date or end_date:
        ds = subset_time(ds, start_date=start_date, end_date=end_date)

    time_freq = xr.infer_freq(ds['time'])
    if sub_daily_agg and (time_freq != 'D'):
        logging.info(f'resampling from {time_freq} to daily {sub_daily_agg}')
        if sub_daily_agg == 'mean':
            ds = ds.resample(time='1D').mean(dim='time')
        elif sub_daily_agg == 'min':
            ds = ds.resample(time='1D').min(dim='time')
        elif sub_daily_agg == 'max':
            ds = ds.resample(time='1D').max(dim='time')
        time_freq = xr.infer_freq(ds['time'])

    ds, cf_var = fix_input_metadata(ds, variable_name, sub_daily_agg)
    
    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    try:
        ds = ds.drop('time_bnds')
    except ValueError:
        pass

    try:
        ds = ds.drop('lat_bnds')
    except ValueError:
        pass

    try:
        ds = ds.drop('lon_bnds')
    except ValueError:
        pass

    return ds, cf_var


def subset_time(ds, start_date=None, end_date=None):
    """Subset the time axis.

    Parameters
    ----------    
    ds : Union[xarray.DataArray, xarray.Dataset]
        Input data.
    start_date : Optional[str]
        Start date of the subset.
        Date string format -- can be year ("%Y"), year-month ("%Y-%m") or year-month-day("%Y-%m-%d").
        Defaults to first day of input data-array.
    end_date : Optional[str]
        End date of the subset.
        Date string format -- can be year ("%Y"), year-month ("%Y-%m") or year-month-day("%Y-%m-%d").
        Defaults to last day of input data-array.

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Subsetted xarray.DataArray or xarray.Dataset
    """

    return ds.sel({'time': slice(start_date, end_date)})


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

