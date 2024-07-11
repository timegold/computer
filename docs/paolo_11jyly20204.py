from astroquery.gaia import Gaia

import pandas as pd
import numpy as np
import sys


def get_gaia_par(input_csv,outputc_csv): 
    #Authenticated access (TAP+ only)

    Gaia.login_gui()
    job = Gaia.upload_table(upload_resource=input_csv,
                            table_name="user_table100", format="csv")


    query=f"SELECT gaia.source_id,ra,ra_error,dec,dec_error,gaia.phot_g_mean_flux,gaia.phot_g_mean_flux_error,gaia.phot_g_mean_mag,gaia.phot_bp_mean_flux,gaia.phot_bp_mean_flux_error,gaia.phot_bp_mean_mag,gaia.phot_rp_mean_flux,gaia.phot_rp_mean_flux_error,gaia.phot_rp_mean_mag \
    FROM gaiadr3.gaia_source AS gaia \
    JOIN user_mmessi01.user_table100 AS ul \
    ON gaia.source_id = ul.source_id \
    WHERE(gaia.source_id = ul.source_id)"


    job = Gaia.launch_job(query=query)
    results = job.get_results()

    frames =(results.to_pandas(),)  
    df_results = pd.concat(frames)
    df_results.head()
    df_results.to_csv(outputc_csv)
    print (df_results)
    
    job = Gaia.delete_user_table("user_table100")

#grep -v none CALSPEC-resultCOORD.csv > test.csv
get_gaia_par('test.csv','calspec_par_PAOLO11-7-24.csv')

sys.exit()


