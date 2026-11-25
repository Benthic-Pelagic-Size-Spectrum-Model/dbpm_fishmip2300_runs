#!/usr/bin/env python3

# Loading libraries
import os
import useful_functions as uf
import xarray as xr
import numpy as np
import pandas as pd
from glob import glob
from distributed import Client, LocalCluster
from multiprocessing import Process, freeze_support

#Start cluster
if __name__ == '__main__':
    freeze_support()

    #Start a cluster
    client = Client(threads_per_worker = 1, memory_limit = 0)
    
    # Define location of DBPM inputs
    base_dir = '/g/data/vf71/fishmip_inputs/fishmip_2300/global_gridded_zarr'

    # Folders to be processed
    exp_names = os.listdir(base_dir)
    esm_names = os.listdir(os.path.join(base_dir, 'picontrol'))
    # exp_esm_folder = glob(os.path.join(base_dir, '*/*'))
    weight_files = glob(os.path.join(base_dir, '*/*/*_bio-weights_*'))

    
    exp_esm_folder = glob(os.path.join(base_dir, f'*/{esm_names[1]}'))
        
    # Define variables for which data will be extracted
    # vars_int = ['input-w20m', 'expc-bot', 'simask', 'tob', 'tos', 'deptho', 
    #             'areacello', 'phyc', 'phypico', 'thetao', 'bio-weights',
    #             'thkcello']
    # vars_int = ['input-w20m', 'expc-bot', 'deptho', 'phyc', 
    #             'phypico', 'thetao', 'bio-weights', 'thkcello']
        
    #List all files to be extracted
    gfdl_out = os.path.join(
        base_dir, 'gridded')
    os.makedirs(gfdl_out, exist_ok = True)
    
    # Vertically integrate phytoplankton inputs up to threshold depth
    for exp in ['ctrlclim', 'obsclim']:
        base_fn = (weight_file.replace('_bio-weights_', '_var_').
            replace('_ctrlclim_', f'_{exp}_'))
        phyc, phypico, temp_ocean = uf.integrating_inputs(
            gfdl_out, exp, thresh_depth = 200, averaging = 'custom', 
            weights = weight_file)
        #Save outputs
        phyc.to_zarr(base_fn.replace('_var_', '_phyc-vint-weighted_'),
                     consolidated = True, mode = 'w')
        phypico.to_zarr(base_fn.replace(
            '_var_', '_phypico-vint-weighted_'), 
                        consolidated = True, mode = 'w')
        temp_ocean.to_zarr(base_fn.replace(
            '_var_', '_ocean-temp-weighted_'), 
                           consolidated = True, mode = 'w')

        #Calculate phytoplankton size distribution and export ratio
        sphy, lphy, er = uf.getExportRatio(gfdl_out, exp)
        #Save outputs
        sphy.to_zarr(base_fn.replace('_var_', '_sphy_'), 
                     consolidated = True, mode = 'w')
        lphy.to_zarr(base_fn.replace('_var_', '_lphy_'),
                     consolidated = True, mode = 'w')
        er.to_zarr(base_fn.replace('_var_', '_er_'),
                   consolidated = True, mode = 'w')
    
    # Calculating slope and intercept
    lphy_files = sorted(glob(os.path.join(gfdl_out, '*_lphy_*')))
    sphy_files = sorted(glob(os.path.join(gfdl_out, '*_sphy_*')))
    for l, s in zip(lphy_files, sphy_files):
        intercept, slope = uf.GetPPIntSlope(sphy_file = s, lphy_file = l)
        #Save outputs
        intercept.to_zarr(l.replace('_lphy_', '_intercept_'), 
                          consolidated = True, mode = 'w')
        slope.to_zarr(l.replace('_lphy_', '_slope_'), 
                      consolidated = True, mode = 'w')
