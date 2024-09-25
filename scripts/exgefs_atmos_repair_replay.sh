#! /usr/bin/env bash


source "${USHgfs}/preamble.sh"

#foutmax=6 #last lead hour to extract 
cd "${DATA}" || exit 1

#COMIN_03 and {COMIN_ATMOS_MASTER} are directory containing the files that we want to extract
#Extract c00 files for f003 from PSL reanalysis data

fnh=03
echo "extracting f${fnh}"
oufile=${DATA}/gefs.t00z.master.grb2f0${fnh}

infile=${COMIN_03}/GFSPRS.GrbF03
if [[ -f "${infile}" ]]; then #check if input file exists before extraction

  ${WGRIB2} "${infile}" | grep "TSNOWP" | ${WGRIB2} -i "${infile}" -grib tmp ||true
  ${WGRIB2} tmp -for "2:2" -append -grib  "${oufile}">/dev/null || true 
 
  ${WGRIB2} "${infile}" | grep ":APCP:surface" | ${WGRIB2} -i "${infile}" -grib tmp ||true
  ${WGRIB2} tmp -for "1:1" -append -grib  "${oufile}">/dev/null || true

  ${WGRIB2} "${infile}" | grep ":ACPCP:surface" | ${WGRIB2} -i "${infile}" -grib tmp ||true
  ${WGRIB2} tmp -for "1:1" -append -grib  "${oufile}">/dev/null || true

  ${WGRIB2} "${infile}" | grep ":NCPCP:surface" | ${WGRIB2} -i "${infile}" -grib tmp ||true
  ${WGRIB2} tmp -for "1:1" -append -grib  "${oufile}">/dev/null || true

  ${WGRIB2} "${infile}" | grep ":HCDC:high cloud layer:0" | ${WGRIB2} -i "${infile}" -append -grib "${oufile}">/dev/null || true
  ${WGRIB2} "${infile}" | grep ":MCDC:middle cloud layer:0" | ${WGRIB2} -i "${infile}" -append -grib "${oufile}">/dev/null || true
  ${WGRIB2} "${infile}" | grep ":LCDC:low cloud layer:0" | ${WGRIB2} -i "${infile}" -append -grib "${oufile}">/dev/null || true

  rm tmp
else
  echo "${infile} does not exist"
  export err=1; err_chk
fi 

export varlist=${varlist_FLXacc03} # Parameter table for f03 acc/ave/min/max variables (PSL data)
infile=${COMIN_03}/GFSFLX.GrbF03
if [[ -f "${infile}" ]]; then #check if input file exists before extraction
    ${WGRIB2} "${infile}" | grep -F -f  "${varlist}" | ${WGRIB2} -i "${infile}" -append -grib "${oufile}">/dev/null || true
else
  echo "${infile} does not exist"
  export err=1; err_chk
fi 
#  done 


#Extract individual member files for f006 master data
fnh=006

echo "extracting f${fnh}"
infile=${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f${fnh}
oufile=${DATA}/gefs.t00z.master.grb2f${fnh}

export varlist=${varlist_masteracc06} # Parameter table for f06 acc/ave/min/max variables

if [[ -f "${infile}" ]]; then #check if input file exists before extraction
#    rm -f ${outfile}/gefs.t00z.master.grb2f${fnh}  #remove outfile if it already exists before extraction

  ${WGRIB2} "${infile}" | grep -F -f "${varlist}" | ${WGRIB2} -i "${infile}" -append -grib  "${oufile}" || true
  ${WGRIB2} "${infile}" | grep "TSNOWP" | ${WGRIB2} -i "${infile}" -grib tmp || true

  ${WGRIB2} tmp -for "2:2" -append -grib  "${oufile}">/dev/null || true 
  ${WGRIB2} tmp -for "1:1" -grib  TSNOWP1.dat || true

else
  echo "${infile} does not exist"
  export err=1; err_chk
fi

 export exec_dir=${HOMEgfs}/exec
# export sorc_dir=${SORCacc}
 export sorc_name=gefs_6h_ave_1mem.x

#   cd $DATA

"${exec_dir}/${sorc_name}" >sorc_name.exe.out
cat sorc_name.exe.out

#output f06
infile=${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f006

${WGRIB2} "${infile}" -match_inv | grep -v -F -f "${varlist}" | wgrib2 -i  "${infile}" -grib out1.grb2 || true
${WGRIB2} out1.grb2 -not "TSNOWP" -grib out2.grb2
   
cat out2.grb2 TSNOWP1.dat gefs.t00z.pgrb2af006 > out3.grb2
mv "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f006" "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f006_org"
mv out3.grb2 "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f006"

rm -fr out1.grb2 out2.grb2 TSNOWP*.dat

#output f03
export varlist=${varlist_masteracc03} # Parameter table for f06 acc/ave/min/max variables
infile=${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f003
#
${WGRIB2} "${infile}" | grep "TSNOWP" | ${WGRIB2} -i "${infile}" -grib TSNOWP2.dat || true
${WGRIB2} TSNOWP2.dat -for "1:1" -grib  out1.grb2 >/dev/null || true
${WGRIB2} out1.grb2 -set_ftime "0-3 hour acc fcst" -grib TSNOWP1.dat 

${WGRIB2} "${infile}" -match_inv | grep -v -F -f "${varlist}" | wgrib2 -i  "${infile}" -grib out2.grb2 || true
${WGRIB2} out2.grb2 -set_ftime "3 hour fcst" -grib out3.grb2

mv "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f003" "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f003_org"
cat out3.grb2 TSNOWP1.dat gefs.t00z.pgrb2af003 > "${COMIN_ATMOS_MASTER}/gefs.t00z.master.grb2f003"

rm -fr out1.grb2 out2.grb2 out3.grb2

exit