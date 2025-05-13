#!/bin/bash
# ERDDAP Dataset XML Generator Script
# Auto-generated from THREDDS catalog
# Generated on: 2025-05-13 13:35:24

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
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/CPC_US_precip/precip.V1.0.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.mean.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.anom.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/icec.mean.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/OISSThires/sst.err.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obil.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dbss_obml.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/dzdt.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/pottmp.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/salt.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sltfl.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/sshg.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/thflx.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/ucur.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/uflx.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vcur.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/godas/vflx.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/air.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/hgt.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/rhum.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/shum.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/omega.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/uwnd.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/vwnd.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.tropp.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/tropopause/pres.tropp.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pr_wtr.eatm.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/slp.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pres.sfc.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/air.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/omega.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/pottmp.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/rhum.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/uwnd.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/vwnd.sig995.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx.sfc.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface/lftx4.sfc.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/ulwrf.ntat.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csulf.ntat.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/csusf.ntat.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/dswrf.ntat.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hcb.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.hct.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lcb.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.lct.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mcb.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/pres.mct.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/tcdc.eatm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/other_gauss/uswrf.ntat.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/air.2m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/skt.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/prate.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shum.2m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/lhtfl.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/shtfl.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uwnd.10m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vwnd.10m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnlf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cfnsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/cprat.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdlf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csdsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/csusf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dlwrf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/dswrf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/gflux.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/icec.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nbdsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nddsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nlwrs.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/nswrs.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pevpr.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/pres.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/runof.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/sfcr.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.0-10cm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/soilw.10-200cm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmax.2m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmin.2m.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.0-10cm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.10-200cm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/tmp.300cm.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uflx.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ugwd.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/ulwrf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/uswrf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vbdsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vddsf.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vflx.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/vgwd.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/surface_gauss/weasd.sfc.gauss.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggwnd.nc' 10080"
  "$ERDDAP_TOOLS/GenerateDatasetsXml.sh EDDGridFromDap 'https://psl.noaa.gov/thredds/dodsC/Aggregations/ncep.reanalysis/pressure/aggvars.nc' 10080"
)

# Run commands in parallel
run_parallel $MAX_JOBS "${COMMANDS[@]}"

echo "All done! Generated XML for ${#COMMANDS[@]} datasets."
