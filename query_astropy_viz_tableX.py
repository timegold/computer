def get_twomassid(mycatalog,outputc_csv): 
    suarez = Vizier(catalog=mycatalog,columns=["*", 'RAJ2000', 'DEJ2000'],row_limit=500,vizier_server='vizier.cds.unistra.fr').query_constraints(RAJ2000=">0 ")[0]
    print(suarez)                       

    frames1 =suarez.to_pandas()
    c=SkyCoord(frames1['RAJ2000'],frames1['DEJ2000'], unit=(u.hourangle, u.deg))
    frames1['RA_deg']=c.ra.degree
    frames1['DEC_deg']=c.dec.degree
    frames1.to_csv('frames1.csv')

    #XMatch only works with coordinates in degrees
    input_table = Table.read('frames1.csv')
    tabletwo = XMatch.query(cat1=input_table,
                     cat2='vizier:II/246/out',
                     max_distance=2.5 * u.arcsec, colRA1='RA_deg',
                     colDec1='DEC_deg')
    frames2 =tabletwo.to_pandas()
    frames2.to_csv(outputc_csv)
