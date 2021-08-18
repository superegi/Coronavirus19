import pandas as pd
import datetime as dt
import numpy as np
import script_importador as si
UT=si.Utilitarios()

class Divisor:
    def __init__(self):
        print('Cargando Fecha de Hoy !')
        self.TS = dt.datetime.now().strftime("%m-%d %H:%M:%S")
        self.hora=dt.datetime.now().strftime('%H:%M:%S')
        print('Fecha Cargada Correctamente!')
        print(UT.TS(), ": ", "*** Inicio Divisor         ***")
        print(UT.TS(), ": ",'"*** Modulos:                            ***"')
        print(UT.TS(), ": ",'"* -CargaData                              *"')
        print(UT.TS(), ": ",'"* -CreaPartes                             *"')  
    
    def CargaData(self):
        print(UT.TS(), ": ", "Cargando Chunk Data")
        self.data=pd.read_csv('export.csv',sep="~", 
                              dayfirst=False, 
                              dtype=str,
                              chunksize=500000,
                              error_bad_lines=False
                              )
        print(UT.TS(), ": ", "Chunk Data Cargada")
    
    def CreaPartes(self):
        print(UT.TS(),": ",'Comienza el Divisor de BDs')
        num=1
        for chunk in self.data:
            print(UT.TS(),": ",'Analizando una parte....')
            chunk["fecha_notificacion"] = pd.to_datetime(chunk["fecha_notificacion"], 
                                                         format='%Y-%m-%d',
                                                         errors='coerce'
                                                         )
            chunk = chunk.loc[chunk.fecha_notificacion > pd.to_datetime('2020-12-31')]
            if len(chunk)>0:
                print(UT.TS(),": ",'Despues de filtro de fecha se detectó datos!')
                print(UT.TS(),": ",'Se procede a filtrar por Region!')
                print("Creando Index1 -> Filtro: 'region'==...")
                index1 = chunk[chunk["region"] == "Región de Valparaíso"].index
                print(UT.TS(),": ","Index BD1 OK")
                
                print(UT.TS(),": ","creando Index2 -> Filtro: 'region_residencia'==...")
                index2 = chunk[chunk["region_residencia"] == "Región de Valparaíso"].index
                print(UT.TS(),": ","Index BD2 OK")
                
                print(UT.TS(),": ","Juntando Index unicos...")
                index3=np.unique(list(index1.values)+list(index2.values))
                print("OK")
                print(UT.TS(),": ","Quintando Duplicados Por Folio...")
                print(UT.TS(),": ","Quitados!")
                print(UT.TS(),": ","Guardando parte:",num)
                UT.guardador(chunk.loc[index3],
                          "./notf_valpo_parte"+str(num),
                          "pkl"
                          )
                print(UT.TS(),": ",'Guardada parte: ',num,'de la BD')
                num+=1
            else:
                print(UT.TS(),": ",'Filtro de fecha no dejó datos')
                print(UT.TS(),": ",'no se guardo una BD')
    