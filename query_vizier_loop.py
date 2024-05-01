# Hide warnings
import warnings
warnings.catch_warnings()
warnings.simplefilter("ignore")

import pyvo
import pandas as pd
from pandas import DataFrame
import numpy as np
import math


def step1(filemy,outcsv): 
 data0 = pd.read_csv(filemy,header=None,sep='\s+')
 data0=data0.rename(columns={0:"name",\
 1:"ra",2:"dec",3:"Sptype",4:"2massID",5:"source_id"})

 frames = ()
 myname=data0['2massID'].to_list()
 nlen=len(myname)
 for  i in range(0, nlen):
     namedr3 = myname[i]
     if(namedr3 != 'none'): 
       tap_service_url = "https://TAPVizieR.cds.unistra.fr/TAPVizieR/tap"  
       tap_service = pyvo.dal.TAPService(tap_service_url)
       query_name = "pippo"
       frames1 = ()
       query="""
         SELECT  "II/246/out"."2MASS", "II/246/out".RAJ2000, "II/246/out".DEJ2000, 
         "II/246/out".Jmag, "II/246/out".e_Jmag, 
         "II/246/out".Hmag, "II/246/out".e_Hmag, 
         "II/246/out".Kmag, "II/246/out".e_Kmag 
         FROM "II/246/out" 
         WHERE "II/246/out"."2MASS" ='"""+namedr3+"'"
       print(query)
       print(type(namedr3))

       tap_result = tap_service.run_async(query)
       frames =frames + (tap_result.to_table().to_pandas(),)
 df_results = pd.concat(frames)
 df_results.head()
 print (df_results)
 df_results.to_csv(outcsv)




data0 = pd.read_csv('wrall2_24.tab',header=None,sep='\s+')
data0=data0.rename(columns={0:"name",\
1:"ra",2:"dec",3:"Sptype",4:"2massID",5:"source_id"})

step1('wrall2_24.tab','data.csv')

