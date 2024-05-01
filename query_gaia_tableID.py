

def get_gaia_par(input_csv,outputc_csv): 
    #Authenticated access (TAP+ only)

    Gaia.login_gui()
    job = Gaia.upload_table(upload_resource=input_csv,
                            table_name="user_table100", format="csv")

    query=f"SELECT  gaia.source_id,gaia.phot_g_mean_mag,gaia.phot_bp_mean_mag,phot_rp_mean_mag,bp_rp,phot_variable_flag,non_single_star,has_xp_continuous,phot_bp_n_obs,phot_rp_n_obs,ra,dec \
    FROM gaiadr3.gaia_source AS gaia \
    JOIN user_mmessi01.user_table100 AS ul \
    ON gaia.source_id = ul.source_id \
    WHERE ( gaia.source_id = ul.source_id)"
    print (query)


    job = Gaia.launch_job(query=query)
    results = job.get_results()

    frames =(results.to_pandas(),)  

    #
    # Contatenate into a pandas.DataFrame
    #
    df_results = pd.concat(frames)
    df_results.head()
    print (df_results)
    
    datacsv = pd.read_csv(input_csv)
    df3=df_results.set_index('source_id').join(datacsv.set_index('source_id'),how='inner')
    # Pandas join on column
    print(df3)
    df3.to_csv(outputc_csv)

    job = Gaia.delete_user_table("user_table100")
