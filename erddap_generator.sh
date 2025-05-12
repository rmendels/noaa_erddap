#!/bin/bash
# ERDDAP Dataset XML Generator Script
# Auto-generated from THREDDS catalog
# Generated on: 2025-05-12 11:21:22

# Stop on errors
set -e

# Function to run commands in parallel with a maximum of N jobs
run_parallel() {
    local max_jobs=$1
    shift
    local cmds=( "$@" )
    local running=0
    local pids=()
    local cmds_index=0
    local cmds_len=${#cmds[@]}

    while (( cmds_index < cmds_len || running > 0 )); do
        # Start jobs while we have slots and commands
        while (( running < max_jobs && cmds_index < cmds_len )); do
            eval "${cmds[cmds_index]}" &
            pids+=($!)
            ((running++))
            ((cmds_index++))
            echo "Started job $cmds_index/$cmds_len (${running} running)"
        done

        # Wait for any job to finish
        if (( running > 0 )); then
            wait -n
            # Find which pid finished
            local alive_pids=()
            for pid in "${pids[@]}"; do
                if kill -0 $pid 2>/dev/null; then
                    alive_pids+=($pid)
                fi
            done
            pids=("${alive_pids[@]}")
            ((running--))
        fi
    done
}

# Variables
ERDDAP_TOOLS="/Users/rmendels/Applications/tomcat_erddap/webapps/erddap/WEB-INF"
MAX_JOBS=4  # Maximum parallel jobs

# Create array of commands
COMMANDS=(
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/CPC_US_precip/precip.V1.0.nc' -datasetID 'AggroCPCprecip' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obil.nc' -datasetID 'AggroGODASobil' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obml.nc' -datasetID 'AggroGODASobml' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dzdt.nc' -datasetID 'AggroGODASdzdt' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/pottmp.nc' -datasetID 'AggroGODASpottmp' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/salt.nc' -datasetID 'AggroGODASsalt' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sltfl.nc' -datasetID 'AggroGODASsltfl' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sshg.nc' -datasetID 'AggroGODASsshg' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/thflx.nc' -datasetID 'AggroGODASthflx' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/ucur.nc' -datasetID 'AggroGODASucur' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/uflx.nc' -datasetID 'AggroGODASuflx' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vcur.nc' -datasetID 'AggroGODASvcur' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vflx.nc' -datasetID 'AggroGODASvflx' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.mean.nc' -datasetID 'SST_OISST_V2_HighRes_Mean' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.anom.nc' -datasetID 'SST_OISST_V2_HighRes_Anom' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/icec.mean.nc' -datasetID 'ICEC_OISST_V2_HighRes' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.err.nc' -datasetID 'ERR_OISST_V2_HighRes_Err' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.tropp.nc' -datasetID 'AggroSampleR1TROPT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/tropopause/pres.tropp.nc' -datasetID 'AggroSampleR1TROPP' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/ulwrf.ntat.gauss.nc' -datasetID 'AggroSampleR1OLR' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csulf.ntat.gauss.nc' -datasetID 'AggroSampleR1CSULFN' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csusf.ntat.gauss.nc' -datasetID 'AggroSampleR1CSUSFN' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/dswrf.ntat.gauss.nc' -datasetID 'AggroSampleR1DSWRFN' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hcb.gauss.nc' -datasetID 'AggroSampleR1PRSHCB' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hct.gauss.nc' -datasetID 'AggroSampleR1PRSHCT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lcb.gauss.nc' -datasetID 'AggroSampleR1PRSLCB' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lct.gauss.nc' -datasetID 'AggroSampleR1PRSLCT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mcb.gauss.nc' -datasetID 'AggroSampleR1PRSMCB' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mct.gauss.nc' -datasetID 'AggroSampleR1PRSMCT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/tcdc.eatm.gauss.nc' -datasetID 'AggroSampleR1TCDC' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/uswrf.ntat.gauss.nc' -datasetID 'AggroSampleR1USNT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/air.nc' -datasetID 'AggroSampleT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/hgt.nc' -datasetID 'AggroSampleH' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/rhum.nc' -datasetID 'AggroSampleR' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/shum.nc' -datasetID 'AggroSampleS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/omega.nc' -datasetID 'AggroSampleO' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/uwnd.nc' -datasetID 'AggroSampleU' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/vwnd.nc' -datasetID 'AggroSampleV' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pr_wtr.eatm.nc' -datasetID 'AggroSampleR1PW' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/slp.nc' -datasetID 'AggroSampleR1SLP' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pres.sfc.nc' -datasetID 'AggroSampleR1SRFP' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.sig995.nc' -datasetID 'AggroSampleR1SIG995T' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/omega.sig995.nc' -datasetID 'AggroSampleR1SIG995W' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pottmp.sig995.nc' -datasetID 'AggroSampleR1SIG995PT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/rhum.sig995.nc' -datasetID 'AggroSampleR1SIG995RH' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/uwnd.sig995.nc' -datasetID 'AggroSampleR1SIG995U' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/vwnd.sig995.nc' -datasetID 'AggroSampleR1SIG995V' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx.sfc.nc' -datasetID 'AggroSampleR1SIG995LFTX' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx4.sfc.nc' -datasetID 'AggroSampleR1SIG995LFTX4' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/air.2m.gauss.nc' -datasetID 'AggroSampleT2M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/skt.sfc.gauss.nc' -datasetID 'AggroSampleSKT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/prate.sfc.gauss.nc' -datasetID 'AggroSamplePRATE' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shum.2m.gauss.nc' -datasetID 'AggroSampleSHUM2M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/lhtfl.sfc.gauss.nc' -datasetID 'AggroSampleShLHTFL' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shtfl.sfc.gauss.nc' -datasetID 'AggroSampleSHTFL' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uwnd.10m.gauss.nc' -datasetID 'AggroSampleU10M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vwnd.10m.gauss.nc' -datasetID 'AggroSampleV10M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnlf.sfc.gauss.nc' -datasetID 'AggroSampleCFNLFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnsf.sfc.gauss.nc' -datasetID 'AggroSampleCFNSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cprat.sfc.gauss.nc' -datasetID 'AggroSampleCPRAT' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdlf.sfc.gauss.nc' -datasetID 'AggroSampleCSDLFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdsf.sfc.gauss.nc' -datasetID 'AggroSampleCSDSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csusf.sfc.gauss.nc' -datasetID 'AggroSampleCSUSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dlwrf.sfc.gauss.nc' -datasetID 'AggroSampleDLWRFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dswrf.sfc.gauss.nc' -datasetID 'AggroSampleDSWRFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/gflux.sfc.gauss.nc' -datasetID 'AggroSampleGFLUX' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/icec.sfc.gauss.nc' -datasetID 'AggroSampleICEC' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nbdsf.sfc.gauss.nc' -datasetID 'AggroSampleNBDSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nddsf.sfc.gauss.nc' -datasetID 'AggroSampleNDDSF' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nlwrs.sfc.gauss.nc' -datasetID 'AggroSampleNLWRSS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nswrs.sfc.gauss.nc' -datasetID 'AggroSampleNSWRSS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pevpr.sfc.gauss.nc' -datasetID 'AggroSamplePEVPR' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pres.sfc.gauss.nc' -datasetID 'AggroSamplePRSSFCG' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/runof.sfc.gauss.nc' -datasetID 'AggroSampleRUNOF' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/sfcr.sfc.gauss.nc' -datasetID 'AggroSampleSFCR' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.0-10cm.gauss.nc' -datasetID 'AggroSampleSOILW010CM' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.10-200cm.gauss.nc' -datasetID 'AggroSampleSOILW10200CM' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmax.2m.gauss.nc' -datasetID 'AggroSampleTMAX2M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmin.2m.gauss.nc' -datasetID 'AggroSampleTMIN2M' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.0-10cm.gauss.nc' -datasetID 'AggroSampleTMP010CM' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.10-200cm.gauss.nc' -datasetID 'AggroSampleTMP10200CM' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.300cm.gauss.nc' -datasetID 'AggroSampleTMP300CM' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uflx.sfc.gauss.nc' -datasetID 'AggroSampleUFLXS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ugwd.sfc.gauss.nc' -datasetID 'AggroSampleUGWDS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ulwrf.sfc.gauss.nc' -datasetID 'AggroSampleULWRFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uswrf.sfc.gauss.nc' -datasetID 'AggroSampleUSWRFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vbdsf.sfc.gauss.nc' -datasetID 'AggroSampleVBDSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vddsf.sfc.gauss.nc' -datasetID 'AggroSampleVDDSFS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vflx.sfc.gauss.nc' -datasetID 'AggroSampleVFLXS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vgwd.sfc.gauss.nc' -datasetID 'AggroSampleVGWDS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/weasd.sfc.gauss.nc' -datasetID 'AggroSampleWEASDS' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggwnd.nc' -datasetID 'AggroWinds' -reloadEveryNMinutes 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh -baseUrl 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggvars.nc' -datasetID 'AggroVars' -reloadEveryNMinutes 10080"
)

# Run commands in parallel
run_parallel $MAX_JOBS "${COMMANDS[@]}"

echo "All done! Generated XML for ${#COMMANDS[@]} datasets."
