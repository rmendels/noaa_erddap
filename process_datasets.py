#!/usr/bin/env python3
# ERDDAP Dataset XML Generator Script
# Auto-generated from THREDDS catalog
# Generated on: 2025-05-15 15:47:18

import os
import sys
import subprocess
import time
import concurrent.futures
from multiprocessing import cpu_count

# Configuration
ERDDAP_TOOLS = "/Users/rmendels/Applications/tomcat_erddap/webapps/erddap/WEB-INF"
LOGS_DIR = "/Users/rmendels/erddapFiles/logs"
MAX_WORKERS = 4  # Maximum parallel processes

# Make sure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

# Define processor function
def process_dataset(name, url, dataset_id, reload_minutes):
    """Process a single dataset with GenerateDatasetsXml.sh"""
    # Create safe filename from dataset name
    safe_name = ''.join(c if c.isalnum() else '_' for c in name)
    safe_name = safe_name[:50]  # Truncate if too long
    
    # Define log file path
    log_file = os.path.join(LOGS_DIR, f"{safe_name}.log")
    
    # Construct command
    cmd = [
        os.path.join(ERDDAP_TOOLS, "GenerateDatasetsXml.sh"),
        "EDDGridFromDap",
        url,
        str(reload_minutes)
    ]
    
    # Execute command and redirect output to log file
    print(f"Processing {name} => {log_file}")
    with open(log_file, 'w') as log:
        try:
            process = subprocess.run(
                cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=True
            )
            return (name, True, "Success")
        except subprocess.CalledProcessError as e:
            return (name, False, f"Error (code {e.returncode})")
        except Exception as e:
            return (name, False, str(e))

# Dataset definitions
datasets = [
    ("NOAA Daily 1/4 degree OISST Mean", "https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.mean.nc", "SST_OISST_V2_HighRes_Mean", 10080),
    ("NOAA Daily 1/4 degree OISST Anomaly", "https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.anom.nc", "SST_OISST_V2_HighRes_Anom", 10080),
    ("NOAA Daily 1/4 degree OISST Sea Ice", "https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/icec.mean.nc", "ICEC_OISST_V2_HighRes", 10080),
    ("NOAA Daily 1/4 degree OISST Errors", "https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.err.nc", "ERR_OISST_V2_HighRes_Err", 10080),
    ("CPC US Precipitation", "https://psl.noaa.gov/thredds/dodsC/Aggregations/CPC_US_precip/precip.V1.0.nc", "AggroCPCprecip", 10080),
    ("GODAS dbss_obil", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obil.nc", "AggroGODASobil", 10080),
    ("GODAS dbss_obml", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obml.nc", "AggroGODASobml", 10080),
    ("GODAS dzdt", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dzdt.nc", "AggroGODASdzdt", 10080),
    ("GODAS pottmp", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/pottmp.nc", "AggroGODASpottmp", 10080),
    ("GODAS salt", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/salt.nc", "AggroGODASsalt", 10080),
    ("GODAS sltfl", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sltfl.nc", "AggroGODASsltfl", 10080),
    ("GODAS sshg", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sshg.nc", "AggroGODASsshg", 10080),
    ("GODAS thflx", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/thflx.nc", "AggroGODASthflx", 10080),
    ("GODAS ucur", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/ucur.nc", "AggroGODASucur", 10080),
    ("GODAS uflx", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/uflx.nc", "AggroGODASuflx", 10080),
    ("GODAS vcur", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vcur.nc", "AggroGODASvcur", 10080),
    ("GODAS vflx", "https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vflx.nc", "AggroGODASvflx", 10080),
    ("R1 Tropopause Temperature", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.tropp.nc", "AggroSampleR1TROPT", 10080),
    ("R1 Tropopause Pressure", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/tropopause/pres.tropp.nc", "AggroSampleR1TROPP", 10080),
    ("R1 pressure level Air Temperature", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/air.nc", "AggroSampleT", 10080),
    ("R1 pressure level Geopotential Height", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/hgt.nc", "AggroSampleH", 10080),
    ("R1 pressure level Relative Humidity", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/rhum.nc", "AggroSampleR", 10080),
    ("R1 pressure level Specific Humidity", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/shum.nc", "AggroSampleS", 10080),
    ("R1 pressure level Vertical Velocity", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/omega.nc", "AggroSampleO", 10080),
    ("R1 pressure level U-wind", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/uwnd.nc", "AggroSampleU", 10080),
    ("R1 pressure level V-wind", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/vwnd.nc", "AggroSampleV", 10080),
    ("R1 Upward Longwave Radiation NTAT", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/ulwrf.ntat.gauss.nc", "AggroSampleR1OLR", 10080),
    ("R1 Clear Sky Upward Longwave Radiation NTAT", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csulf.ntat.gauss.nc", "AggroSampleR1CSULFN", 10080),
    ("R1 Clear Sky Upward Shortwave Radiation NTAT", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csusf.ntat.gauss.nc", "AggroSampleR1CSUSFN", 10080),
    ("R1 Downward Shortwave Radiation NTAT", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/dswrf.ntat.gauss.nc", "AggroSampleR1DSWRFN", 10080),
    ("R1 Pressure High Cloud Bottom", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hcb.gauss.nc", "AggroSampleR1PRSHCB", 10080),
    ("R1 Pressure High Cloud Top", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hct.gauss.nc", "AggroSampleR1PRSHCT", 10080),
    ("R1 Pressure Low Cloud Bottom", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lcb.gauss.nc", "AggroSampleR1PRSLCB", 10080),
    ("R1 Pressure Low Cloud Top", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lct.gauss.nc", "AggroSampleR1PRSLCT", 10080),
    ("R1 Pressure Middle Cloud Bottom", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mcb.gauss.nc", "AggroSampleR1PRSMCB", 10080),
    ("R1 Pressure Middle Cloud Top", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mct.gauss.nc", "AggroSampleR1PRSMCT", 10080),
    ("R1 Total Clouds Entire Atmosphere", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/tcdc.eatm.gauss.nc", "AggroSampleR1TCDC", 10080),
    ("R1 Upward Shortwave Radiation NTAT", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/uswrf.ntat.gauss.nc", "AggroSampleR1USNT", 10080),
    ("R1 Precipitable Water EATM", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pr_wtr.eatm.nc", "AggroSampleR1PW", 10080),
    ("R1 Sea Level Pressure", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/slp.nc", "AggroSampleR1SLP", 10080),
    ("R1 Surface Pressure", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pres.sfc.nc", "AggroSampleR1SRFP", 10080),
    ("R1 Sigma 0.995 Temperature", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.sig995.nc", "AggroSampleR1SIG995T", 10080),
    ("R1 Sigma 0.995 Omega", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/omega.sig995.nc", "AggroSampleR1SIG995W", 10080),
    ("R1 Sigma 0.995 Potential Temperature", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pottmp.sig995.nc", "AggroSampleR1SIG995PT", 10080),
    ("R1 Sigma 0.995 Relative Humidity", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/rhum.sig995.nc", "AggroSampleR1SIG995RH", 10080),
    ("R1 Sigma 0.995 U-wind", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/uwnd.sig995.nc", "AggroSampleR1SIG995U", 10080),
    ("R1 Sigma 0.995 V-wind", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/vwnd.sig995.nc", "AggroSampleR1SIG995V", 10080),
    ("R1 Surface Lifting Index", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx.sfc.nc", "AggroSampleR1SIG995LFTX", 10080),
    ("R1 Surface 4-layer Lifting Index", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx4.sfc.nc", "AggroSampleR1SIG995LFTX4", 10080),
    ("R1 Air Temperature 2m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/air.2m.gauss.nc", "AggroSampleT2M", 10080),
    ("R1 Skin Temperature", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/skt.sfc.gauss.nc", "AggroSampleSKT", 10080),
    ("R1 Precipitation Rate", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/prate.sfc.gauss.nc", "AggroSamplePRATE", 10080),
    ("R1 Specific Humidity 2m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shum.2m.gauss.nc", "AggroSampleSHUM2M", 10080),
    ("R1 Latent Heat Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/lhtfl.sfc.gauss.nc", "AggroSampleShLHTFL", 10080),
    ("R1 Sensible Heat Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shtfl.sfc.gauss.nc", "AggroSampleSHTFL", 10080),
    ("R1 U-Wind 10m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uwnd.10m.gauss.nc", "AggroSampleU10M", 10080),
    ("R1 V-wind 10m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vwnd.10m.gauss.nc", "AggroSampleV10M", 10080),
    ("R1 Cloud Forcing Net Longwave Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnlf.sfc.gauss.nc", "AggroSampleCFNLFS", 10080),
    ("R1 Cloud Forcing Net Solar Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnsf.sfc.gauss.nc", "AggroSampleCFNSFS", 10080),
    ("R1 Convective Precipitation Rate", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cprat.sfc.gauss.nc", "AggroSampleCPRAT", 10080),
    ("R1 Clear Sky Downward Longwave Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdlf.sfc.gauss.nc", "AggroSampleCSDLFS", 10080),
    ("R1 Clear Sky Downward Shortwave Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdsf.sfc.gauss.nc", "AggroSampleCSDSFS", 10080),
    ("R1 Clear Sky Upward Shortwave Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csusf.sfc.gauss.nc", "AggroSampleCSUSFS", 10080),
    ("R1 Downward Longwave Radiation Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dlwrf.sfc.gauss.nc", "AggroSampleDLWRFS", 10080),
    ("R1 Downward Shortwave Radiation Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dswrf.sfc.gauss.nc", "AggroSampleDSWRFS", 10080),
    ("R1 Skin Ground Heat Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/gflux.sfc.gauss.nc", "AggroSampleGFLUX", 10080),
    ("R1 Ice Concentration", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/icec.sfc.gauss.nc", "AggroSampleICEC", 10080),
    ("R1 Near IR Beam Downward Solar Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nbdsf.sfc.gauss.nc", "AggroSampleNBDSFS", 10080),
    ("R1 Near IR Diffuse Downward Solar Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nddsf.sfc.gauss.nc", "AggroSampleNDDSF", 10080),
    ("R1 Net Longwave Radiation Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nlwrs.sfc.gauss.nc", "AggroSampleNLWRSS", 10080),
    ("R1 Net Shortwave Radiation Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nswrs.sfc.gauss.nc", "AggroSampleNSWRSS", 10080),
    ("R1 Potential Evaporation Rate", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pevpr.sfc.gauss.nc", "AggroSamplePEVPR", 10080),
    ("R1 Pressure forecast", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pres.sfc.gauss.nc", "AggroSamplePRSSFCG", 10080),
    ("R1 Water Runoff", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/runof.sfc.gauss.nc", "AggroSampleRUNOF", 10080),
    ("R1 Surface Roughness", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/sfcr.sfc.gauss.nc", "AggroSampleSFCR", 10080),
    ("R1 Volumetric Soil Moisture 0-10cm", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.0-10cm.gauss.nc", "AggroSampleSOILW010CM", 10080),
    ("R1 Volumetric Soil Moisture 10-200cm", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.10-200cm.gauss.nc", "AggroSampleSOILW10200CM", 10080),
    ("R1 Maximum Temperature 2m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmax.2m.gauss.nc", "AggroSampleTMAX2M", 10080),
    ("R1 Minumum Temperature 2m", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmin.2m.gauss.nc", "AggroSampleTMIN2M", 10080),
    ("R1 Temperature 0-10cm", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.0-10cm.gauss.nc", "AggroSampleTMP010CM", 10080),
    ("R1 Temperature 10-200cm", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.10-200cm.gauss.nc", "AggroSampleTMP10200CM", 10080),
    ("R1 Temperature 300cm", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.300cm.gauss.nc", "AggroSampleTMP300CM", 10080),
    ("R1 Momentum Flux U-component", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uflx.sfc.gauss.nc", "AggroSampleUFLXS", 10080),
    ("R1 Zonal Gravity Wave Stress", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ugwd.sfc.gauss.nc", "AggroSampleUGWDS", 10080),
    ("R1 Upward Longwave Radiation Surface", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ulwrf.sfc.gauss.nc", "AggroSampleULWRFS", 10080),
    ("R1 Upward Shortwave Radiation Surface", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uswrf.sfc.gauss.nc", "AggroSampleUSWRFS", 10080),
    ("R1 Visible Beam Downward Solar Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vbdsf.sfc.gauss.nc", "AggroSampleVBDSFS", 10080),
    ("R1 Visible Diffuse Downward Solar Flux", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vddsf.sfc.gauss.nc", "AggroSampleVDDSFS", 10080),
    ("R1 Momentum Flux V-component", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vflx.sfc.gauss.nc", "AggroSampleVFLXS", 10080),
    ("R1 Meridional Gravity Wave Stress", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vgwd.sfc.gauss.nc", "AggroSampleVGWDS", 10080),
    ("R1 Water Equiv. of Accum Snow Depth", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/weasd.sfc.gauss.nc", "AggroSampleWEASDS", 10080),
    ("R1 pressure level Winds (U,V,W) ", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggwnd.nc", "AggroWinds", 10080),
    ("R1 Aggregated Variables (T,Z,U,V,W,RH,SLP)", "https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggvars.nc", "AggroVars", 10080),
]

def main():
    # Keep track of overall stats
    total = len(datasets)
    successful = 0
    failed = 0
    
    start_time = time.time()
    print(f"Processing {total} datasets with {MAX_WORKERS} workers...")
    
    # Process datasets in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        futures = [executor.submit(process_dataset, name, url, dataset_id, reload_mins) 
                  for name, url, dataset_id, reload_mins in datasets]
        
        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            name, success, message = future.result()
            if success:
                successful += 1
                print(f"[{i+1}/{total}] ✓ {name}: {message}")
            else:
                failed += 1
                print(f"[{i+1}/{total}] ✗ {name}: {message}")
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.2f} seconds")
    print(f"Successful: {successful}/{total}")
    print(f"Failed: {failed}/{total}")
    print(f"\nLog files are located in: {LOGS_DIR}")
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
