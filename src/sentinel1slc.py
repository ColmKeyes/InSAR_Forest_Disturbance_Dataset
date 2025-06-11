# -*- coding: utf-8 -*-
"""
This script provides interface to Sentinel-1 preprocessing through SNAP's Graph Processing Framework defined in Javascript
"""
"""
@Time    : 07/12/2022 13:39
@Author  : Colm Keyes
@Email   : keyesco@tcd.ie
@File    : Sentinel-1 SLC Preprocessing
"""

###########
## Example code:
## https://github.com/wajuqi/Sentinel-1-preprocessing-using-Snappy
## Snappy requires python version 3.6 or below.
###########

import datetime
import time
from esa_snappy import ProductIO
from esa_snappy import HashMap
## Garbage collection to release memory
import os, gc
from esa_snappy import GPF
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

##############
## steps needed are:
## TopSAR-Split
## Apply-Orbit-file
## Back-Geocoding
## Coherence
## TOPSAR-Deburst
## Terrain-Correction
## Write
##############


def topsar_split(source, pols, iw_swath=None, first_burst_index=None, last_burst_index=None):
    """
    TOPSAR Split operation - now handles None parameters for full scene processing
    """
    print('\tOperator-TOPSAR-Split...')
    parameters = HashMap()
    
    # Only set swath parameters if specified (for large area processing, process all swaths)
    if iw_swath is not None:
        parameters.put('subswath', iw_swath)
    if first_burst_index is not None:
        parameters.put('firstBurstIndex', first_burst_index)
    if last_burst_index is not None:
        parameters.put('lastBurstIndex', last_burst_index)
        
    parameters.put('selectedPolarisations', pols)
    output = GPF.createProduct('TOPSAR-Split', parameters, source)
    return output


def apply_orbit_file(source):
    print('\tApply orbit file...')
    parameters = HashMap()
    parameters.put('orbitType', 'Sentinel Precise (Auto Download)')
    parameters.put('polyDegree', '3')
    parameters.put('continueOnFail', 'false')
    print(source.getBand(source.getBandNames()[0]))
    output = GPF.createProduct('Apply-Orbit-File', parameters, source)
    return output


def back_geocoding(sources):
    print('\tOperator-Back-Geocoding...')
    parameters = HashMap()
    parameters.put('demName', 'SRTM 3Sec')
    parameters.put('externalDEMNoDataValue', 0.0)
    print(sources[0].getBand(sources[0].getBandNames()[0]))
    parameters.put('demResamplingMethod', 'BILINEAR_INTERPOLATION')
    parameters.put('maskOutAreaWithoutElevation', 'false')
    parameters.put('outputRangeAzimuthOffset', 'false')
    parameters.put('outputDerampDemodPhase', 'false')
    parameters.put('disableReramp', 'false')
    output = GPF.createProduct('Back-Geocoding', parameters, sources)
    return output


def thermal_noise_reduction(source, pols):
    print('\tOperator-Thermal-Noise-Removal...')
    parameters = HashMap()
    parameters.put('selectedPolarisations', pols)
    parameters.put('removeThermalNoise', True)
    output = GPF.createProduct('ThermalNoiseRemoval', parameters, source)
    return output


def calibration_(source, pols):
    print('\tOperator-Radiometric-Calibration...')
    parameters = HashMap()
    parameters.put('outputSigmaBand', True)
    parameters.put('outputGammaBand', False)
    parameters.put('outputBetaBand', False)
    parameters.put('selectedPolarisations', pols)
    output = GPF.createProduct('Calibration', parameters, source)
    return output


def coherence_(source, coh_window_size):
    print('\tOperator-Coherence...')
    parameters = HashMap()
    parameters.put('cohWinAz', coh_window_size[0])  # 3)
    parameters.put('cohWinRg', coh_window_size[1])  # 15)
    print(source.getBand(source.getBandNames()[0]))
    parameters.put('subtractFlatEarthPhase', False)
    parameters.put('srpPolynomialDegree', 5)
    parameters.put('srpNumberPoints', 501)
    parameters.put('orbitDegree', 3)
    parameters.put('subtractTopographicPhase', True)
    parameters.put('demName', 'SRTM 3Sec')
    parameters.put('externalDEMNoDataValue', 0.0)
    parameters.put('externalDEMApplyEGM', True)
    parameters.put('tileExtensionPercent', '100')
    parameters.put('singleMaster', True)
    # parameters.put('subtractTopographicPhase',True)
    parameters.put('squarePixel', False)
    output = GPF.createProduct('Coherence', parameters, source)
    return output


def speckle_filtering(source, filter, filter_size):
    print('\tSpeckle filtering...')
    parameters = HashMap()
    parameters.put('filter', filter)  # 'Lee')
    parameters.put('filterSizeX', filter_size[0])  # 5)
    parameters.put('filterSizeY', filter_size[1])  # 5)
    output = GPF.createProduct('Speckle-Filter', parameters, source)
    return output


def terrain_correction(source, coh_window_size, sentinel1_spacing):
    print('\tTerrain correction...')
    parameters = HashMap()
    parameters.put('demName', 'SRTM 3Sec')
    parameters.put('standardGridOriginX', 0.0)
    parameters.put('standardGridOriginY', 0.0)
    parameters.put('externalDEMApplyEGM', True)
    parameters.put('externalDEMNoDataValue', 0.0)
    parameters.put('nodataValueAtSea', 'True')
    parameters.put('auxFile', 'Latest Auxiliary File')
    band_names = source.getBandNames()
    parameters.put('sourceBands', band_names[0])
    parameters.put('imgResamplingMethod', 'BILINEAR_INTERPOLATION')
    parameters.put('demResamplingMethod', 'BILINEAR_INTERPOLATION')
    # parameters.put("alignToStandardGrid", False)#True)
    # parameters.put('mapProjection', "AUTO:42001")  # default is WGS84
    # parameters.put('saveProjectedLocalIncidenceAngle', True)
    parameters.put('saveSelectedSourceBand', True)
    parameters.put('pixelSpacingInMeter', np.double(np.round(sentinel1_spacing[0] * coh_window_size[0])))
    output = GPF.createProduct('Terrain-Correction', parameters, source)
    return output


def multi_look(source, window_size):
    print('\tOperator-Multilook...')
    parameters = HashMap()
    parameters.put('nAzLooks', window_size[0])
    parameters.put('nRgLooks', window_size[1])
    parameters.put('outputIntensity', True)
    parameters.put('grSquarePixel', False)
    output = GPF.createProduct('Multilook', parameters, source)
    return output


def topsar_deburst(source, pols):
    print('\tOperator-TOPSAR-Deburst...')
    parameters = HashMap()
    parameters.put('selectedPolarisations', pols)
    output = GPF.createProduct('TOPSAR-Deburst', parameters, source)
    return output


def plotBand(product, band, vmin, vmax):
    band = product.getBand(band)
    w = band.getRasterWidth()
    h = band.getRasterHeight()
    print(w, h)
    band_data = np.zeros(w * h, np.float32)
    band.readPixels(0, 0, w, h, band_data)
    band_data.shape = h, w
    width = 12
    height = 12
    plt.figure(figsize=(width, height))
    imgplot = plt.imshow(band_data, cmap=plt.cm.binary, vmin=vmin, vmax=vmax)
    return imgplot


def main(pols,
         iw_swath,
         first_burst_index,
         last_burst_index,
         coh_window_size,
         mode,
         speckle_filter,
         speckle_filter_size,
         product_type,
         outpath,
         SLC_path=None,
         path_asf_csv=None
         ):
    
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    folder_paths = []
    sentinel1_spacing = [14.04, 3.68]
    
    # Read the pairs CSV with new structure
    pairs_csv = pd.read_csv(path_asf_csv)
    print(f"Processing {len(pairs_csv)} pairs from {path_asf_csv}")
    
    for idx, pair in pairs_csv.iterrows():
        # Extract master and slave file IDs from the new CSV structure
        master_file_id = pair['master_id']
        slave_file_id = pair['slave_id']
        
        print(f"\nProcessing pair {idx + 1}/{len(pairs_csv)}")
        print(f"Master: {master_file_id}")
        print(f"Slave: {slave_file_id}")
        print(f"Baseline: {pair['perp_baseline']:.2f}m, Temporal: {pair['temp_baseline']} days")

        # Construct file paths for master and slave SLC files
        master_path = os.path.join(SLC_path, f"{master_file_id}.zip")
        slave_path = os.path.join(SLC_path, f"{slave_file_id}.zip")
        
        # Check if files exist
        if not os.path.exists(master_path):
            print(f"Warning: Master file not found: {master_path}")
            continue
        if not os.path.exists(slave_path) and mode == 'coherence':
            print(f"Warning: Slave file not found: {slave_path}")
            continue
            
        gc.enable()
        gc.collect()
        loopstarttime = str(datetime.datetime.now())
        print('Start time:', loopstarttime)
        start_time = time.time()

        if mode == 'coherence':
            # Load master and slave products
            sentinel_1_1 = ProductIO.readProduct(master_path)
            sentinel_1_2 = ProductIO.readProduct(slave_path)

            width = sentinel_1_1.getSceneRasterWidth()
            print("Width: {} px".format(width))
            height = sentinel_1_1.getSceneRasterHeight()
            print("Height: {} px".format(height))
            name = sentinel_1_1.getName()
            print("Name: {}".format(name))
            band_names = sentinel_1_1.getBandNames()
            print("Band names: {}".format(", ".join(band_names)))

            # TOPSAR Split for both master and slave (process all swaths if iw_swath is None)
            topsarsplit_1 = topsar_split(sentinel_1_1, pols, iw_swath, first_burst_index, last_burst_index)
            topsarsplit_2 = topsar_split(sentinel_1_2, pols, iw_swath, first_burst_index, last_burst_index)
            
            # Apply orbit files
            applyorbit_1 = apply_orbit_file(topsarsplit_1)
            applyorbit_2 = apply_orbit_file(topsarsplit_2)
            
            # Back-geocoding
            backgeocoding = back_geocoding([applyorbit_1, applyorbit_2])
            
            # Coherence calculation
            coherence = coherence_(backgeocoding, coh_window_size)
            
            # TOPSAR Deburst
            topsardeburst = topsar_deburst(coherence, pols)
            
            # Terrain correction
            terraincorrection = terrain_correction(topsardeburst, coh_window_size, sentinel1_spacing)

            del applyorbit_1, applyorbit_2, topsarsplit_1, topsarsplit_2, backgeocoding, coherence, topsardeburst

        elif mode == 'backscatter':
            # Load master product only for backscatter
            sentinel_1_1 = ProductIO.readProduct(master_path)

            width = sentinel_1_1.getSceneRasterWidth()
            print("Width: {} px".format(width))
            height = sentinel_1_1.getSceneRasterHeight()
            print("Height: {} px".format(height))
            name = sentinel_1_1.getName()
            print("Name: {}".format(name))
            band_names = sentinel_1_1.getBandNames()
            print("Band names: {}".format(", ".join(band_names)))

            # Thermal noise reduction
            thermalnoisereduction = thermal_noise_reduction(sentinel_1_1, pols)
            
            # TOPSAR Split
            topsarsplit_1 = topsar_split(thermalnoisereduction, pols, iw_swath, first_burst_index, last_burst_index)
            
            # Apply orbit file
            applyorbit_1 = apply_orbit_file(topsarsplit_1)
            
            # Calibration
            calibration = calibration_(applyorbit_1, pols)
            
            # TOPSAR Deburst
            topsardeburst = topsar_deburst(calibration, pols)
            
            # Multi-look
            multilook = multi_look(topsardeburst, coh_window_size)
            
            # Terrain correction
            terraincorrection = terrain_correction(multilook, coh_window_size, sentinel1_spacing)
            
            # Speckle filtering
            speckle = speckle_filtering(terraincorrection, speckle_filter, speckle_filter_size)

            del thermalnoisereduction, applyorbit_1, topsarsplit_1, calibration, topsardeburst, multilook

        print("Writing output...")

        if mode == 'coherence':
            # Create output filename based on new naming convention
            master_date = master_file_id.split('_')[4][:8]  # Extract date from filename
            slave_date = slave_file_id.split('_')[4][:8]    # Extract date from filename
            
            write_tiff_path = os.path.join(
                outpath, 
                f"{master_date}_{slave_date}_pol_{pols}_coherence_window_{int(sentinel1_spacing[0] * coh_window_size[0])}"
            )
            
            if not os.path.exists(write_tiff_path + '.tif'):
                ProductIO.writeProduct(terraincorrection, write_tiff_path, product_type)
                print(f"Saved: {write_tiff_path}.tif")
            else:
                print(f"File already exists: {write_tiff_path}.tif")
                
            sentinel_1_1.dispose()
            sentinel_1_1.closeIO()
            sentinel_1_2.dispose()
            sentinel_1_2.closeIO()
            del terraincorrection

        elif mode == "backscatter":
            # Create output filename for backscatter
            master_date = master_file_id.split('_')[4][:8]  # Extract date from filename
            
            write_tiff_path = os.path.join(
                outpath,
                f"{master_date}_pol_{pols}_backscatter_multilook_window_{int(sentinel1_spacing[0] * coh_window_size[0])}"
            )
            
            if not os.path.exists(write_tiff_path + '.tif'):
                ProductIO.writeProduct(speckle, write_tiff_path, product_type)
                print(f"Saved: {write_tiff_path}.tif")
            else:
                print(f"File already exists: {write_tiff_path}.tif")
                
            sentinel_1_1.dispose()
            sentinel_1_1.closeIO()
            del speckle

        print('Processing completed.')
        print("--- %s seconds ---" % (time.time() - start_time))

# snappy thermal noise doesn't work for coherence
