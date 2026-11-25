#!/usr/bin/env python3

# Loading libraries
import os
from glob import glob
import re
import useful_functions as uf
import dask
from distributed import Client, LocalCluster
from multiprocessing import freeze_support

# This script pre-processes outputs from ocean models prior to their use as 
# forcings in DBPM. Ocean model outputs are stored as zarr files to process 
# data faster. Additionally, the detritus export to the benthos (export ratio)
# is calculated

#Base folder where GFDL outputs are stored 
base_dir_in = '/g/data/vf71/fishmip_inputs/fishmip_2300/global'
base_dir_out = base_dir_in.replace('global', 'global_gridded_zarr')

# #Define experiments and resolution
picontrol_files = glob(os.path.join(base_dir_in, 'picontrol/*/*.nc'))
other_exp_files = []
for exp in ['hist', 'ssp']:
    other_exp_files.extend(glob(os.path.join(base_dir_in, f'{exp}*/*/*.nc')))


def esm_input_process(filename, start_year, end_year):
    folder_out = (os.path.dirname(filename).
        replace('/global/', '/global_gridded_zarr/'))
    #Check output folder exists
    os.makedirs(folder_out, exist_ok = True)
    #Define output path
    esm_out = os.path.join(
        folder_out, os.path.basename(filename).replace('.nc', '.zarr'))

    if start_year is not None:
        esm_out = re.sub('_[0-9]{4}_', f'_{start_year}_', esm_out)
    if end_year is not None:
        esm_out = re.sub('_[0-9]{4}\\.', f'_{end_year}.', esm_out) 
    
    #Apply function
    # Use a local threaded scheduler inside this worker task, so the dask
    # operations in netcdf_to_zarr do not submit new tasks back to the cluster
    with dask.config.set(scheduler = 'threads'):
        uf.netcdf_to_zarr(filename, path_out = esm_out,
                          start_year = start_year, end_year = end_year)
  
def weights_calc(input_folder):
    # Define output folder
    folder_out = (input_folder.
        replace('/global/', '/global_gridded_zarr/'))

    # Get phyc and thkcello file paths
    [phyc_fn] = glob(os.path.join(input_folder, '*_phyc_*'))
    [depth_fn] = glob(os.path.join(input_folder, '*_thkcello_*'))

    # Calculate mean over reference period
    with dask.config.set(scheduler = 'threads'):
        phyc = (uf.netcdf_to_zarr(
            phyc_fn, start_year = 1961, end_year = 1980).mean('time').
            fillna(0))
        depths = (uf.netcdf_to_zarr(
            depth_fn, start_year = 1961, end_year = 1980).mean('time').
            fillna(0))

    # Calculate weights        
    weights = phyc*depths
    weights.name = 'weights'
    weights = weights.drop_attrs()
    weights = weights.assign_attrs({
        'long_name': 'Climatological mean (1961-1980) biomass weighting'})
    
    #Save outputs
    fn_weights = os.path.join(
        folder_out, (re.sub('_[0-9]{4}\\.', '_1980.', 
                            re.sub('_[0-9]{4}_', '_1961_', os.path.basename(phyc_fn))).
            replace('.nc', '.zarr')).replace('_phyc_', '_bio-weights_'))
    weights.drop_encoding().to_zarr(
        fn_weights, consolidated = True, mode = 'w')


def det_apply(expc_bot_files):
    fn_out = expc_bot_files.replace('_expc-bot_', '_input-w20m_')
    
    # Transforming expc-bot (mol m-2 s-1) to input_w (gWW m-3 yr-1)
    input_w = uf.detrital_input_seafloor(expc_bot_files, benthic_habitat_depth = 20)
    # Save outputs
    input_w.drop_encoding().to_zarr(fn_out, consolidated = True, mode = 'w')
    
    
#Start cluster
if __name__ == '__main__':
    freeze_support()

    #Start a cluster
    cluster = LocalCluster(threads_per_worker = 4)
    client = Client(cluster)
    client.upload_file(
        '/g/data/vf71/la6889/dbpm_fishmip2300_runs/scripts/useful_functions.py')

    # Process inputs
    futures = client.map(esm_input_process, picontrol_files, start_year = 1650,
                         end_year = 1849)
    futures += client.map(esm_input_process, other_exp_files,
                          start_year = None, end_year = None)
   
    weights_future = client.map(
        weights_calc, glob(os.path.join(base_dir_in, 'picontrol/*')))

    det_future = client.map(det_apply, glob(os.path.join(base_dir_out, '*/*/*expc-bot*')))

    depth_future = client.map(uf.calc_tot_depth, glob(os.path.join(base_dir_out, '*/*/*thkcello*')))
    
    # Block until all tasks are done (raises an error if any task fails)
    res1, res2, res3, res4 = client.gather([futures, weights_future, det_future, depth_future])
        
    # Shut down cluster cleanly
    client.close()
    cluster.close()
    
        
