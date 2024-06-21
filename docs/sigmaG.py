import pyvo
import pandas as pd
#from zero_point import zpt
#import numpy

tap_service_url = "https://gaia.ari.uni-heidelberg.de/tap"
tap_service = pyvo.dal.TAPService(tap_service_url)



f = open('gaiapar_rsg.tab', 'r')
frames7 = ()
for line in f:
    line = line.strip()
    columns = line.split()
    namedr3 = columns[0]  #couln4 -1=3 ;new EDR3 Identifiers
    print (namedr3)

    # Submit queries
    query="SELECT range_mag_g_fov, std_dev_mag_g_fov,trimmed_range_mag_g_fov, source_id AS  source_id FROM gaiadr3.vari_summary   WHERE source_id = "+namedr3
    print (query)
    if namedr3 != 'none': 
      tap_result = tap_service.run_sync(query)                 
    if namedr3 != 'none':
      frames7 =frames7 + (tap_result.to_table().to_pandas(),)  
##
## Contatenate into a pandas.DataFrame
##
df_results = pd.concat(frames7)
df_results.head()
#print (df_results)
df_results.to_csv('sigmaG.csv')

