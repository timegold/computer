def get_gaiaid(input_csv,outputc_csv): 
    tap_service_url = "https://gaia.ari.uni-heidelberg.de/tap"
    tap_service = pyvo.dal.TAPService(tap_service_url)

    datacsv = pd.read_csv(input_csv)
    data = []
    frames = ()
    id3=datacsv['2MASS']
    nlen=len(id3)


    for  i in range(1, nlen):
       nametwo = str(id3[i])
# Submit queries
       query=f"SELECT source_id,  clean_tmass_psc_xsc_oid, original_ext_source_id  FROM gaiadr3.tmass_psc_xsc_best_neighbour    WHERE original_ext_source_id LIKE '{nametwo}' "
       print (query)
       if nametwo != 'none': 
          tap_result = tap_service.run_sync(query)                 
       if nametwo != 'none': 
          frames =frames + (tap_result.to_table().to_pandas(),) #turple
#
# Contatenate into a pandas.DataFrame
#
    df_results = pd.concat(frames)
    df_results.head()
    print (type(df_results))  #<class 'pandas.core.frame.DataFrame'>

    df3=df_results.set_index('original_ext_source_id').join(datacsv.set_index('2MASS'),how='inner')
# Pandas join on column
    print(df3)
    
    df3.to_csv(outputc_csv)
