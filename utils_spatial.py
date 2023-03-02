"""Utilities for spatial selection and aggregation."""


def subset_lat(ds, lat_bnds):
    """Select grid points that fall within latitude bounds.

    Parameters
    ----------
    ds : Union[xarray.DataArray, xarray.Dataset]
        Input data
    lat_bnds : list
        Latitude bounds: [south bound, north bound]

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Subsetted xarray.DataArray or xarray.Dataset
    """

    if 'latitude' in ds.dims:
        ds = ds.rename({'latitude': 'lat'})

    south_bound, north_bound = lat_bnds
    assert -90 <= south_bound <= 90, "Valid latitude range is [-90, 90]"
    assert -90 <= north_bound <= 90, "Valid latitude range is [-90, 90]"
    
    lat_axis = ds['lat'].values
    if lat_axis[-1] > lat_axis[0]:
        # monotonic increasing lat axis (e.g. -90 to 90)
        ds = ds.sel({'lat': slice(south_bound, north_bound)})
    else:
        # monotonic decreasing lat axis (e.g. 90 to -90)
        ds = ds.sel({'lat': slice(north_bound, south_bound)})

    return ds


def avoid_cyclic(ds, west_bound, east_bound):
    """Alter longitude axis if requested bounds straddle cyclic point"""

    west_bound_360 = (west_bound + 360) % 360
    east_bound_360 = (east_bound + 360) % 360
    west_bound_180 = ((west_bound + 180) % 360) - 180
    east_bound_180 = ((east_bound + 180) % 360) - 180
    if east_bound_360 < west_bound_360:
        ds = ds.assign_coords({'lon': ((ds['lon'] + 180) % 360) - 180})
        ds = ds.sortby(ds['lon'])
    elif east_bound_180 < west_bound_180:
        ds = ds.assign_coords({'lon': (ds['lon'] + 360) % 360}) 
        ds = ds.sortby(ds['lon'])

    return ds


def subset_lon(ds, lon_bnds):
    """Select grid points that fall within longitude bounds.

    Parameters
    ----------
    ds : Union[xarray.DataArray, xarray.Dataset]
        Input data
    lon_bnds : list
        Longitude bounds: [west bound, east bound]

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Subsetted xarray.DataArray or xarray.Dataset
    """

    if 'longitude' in ds.dims:
        ds = ds.rename({'longitude': 'lon'})
    assert ds['lon'].values.max() > ds['lon'].values.min()

    west_bound, east_bound = lon_bnds

    ds = avoid_cyclic(ds, west_bound, east_bound)

    lon_axis_max = ds['lon'].values.max()
    lon_axis_min = ds['lon'].values.min()
    if west_bound > lon_axis_max:
        west_bound = west_bound - 360
        assert west_bound <= lon_axis_max
    if east_bound > lon_axis_max:
        east_bound = east_bound - 360
        assert east_bound <= lon_axis_max
    if west_bound < lon_axis_min:
        west_bound = west_bound + 360
        assert west_bound >= lon_axis_min
    if east_bound < lon_axis_min:
        east_bound = east_bound + 360
        assert east_bound >= lon_axis_min

    ds = ds.sel({'lon': slice(west_bound, east_bound)})

    return ds

