import pandas as pd
import datetime as dt
import script_importador as si
UT=si.Utilitarios()

class Filtrador:
    def __init__(self):
        print('Cargando Fecha de Hoy !')
        self.TS = dt.datetime.now().strftime("%y-%m-%d %H:%M:%S")
        self.hora=dt.datetime.now().strftime('%H:%M:%S')
        print('Fecha Cargada Correctamente!')
        print(UT.TS(), ": ", "*** Inicio filtrador de BD de Not.  Nacional***")
        print(UT.TS(), ": ", "*** Este objeto cuenta con las clases:      ***")
        print(UT.TS(), ": ", "*  -cargadorBD(BD)                            *")
        print(UT.TS(), ": ", "*  -FiltraGuarda11                            *")
        print(UT.TS(), ": ", "*  -FiltraGuarda60                            *")
        print(UT.TS(), ": ", "*  -duplicados                                *")
        print(UT.TS(), ": ", "*  -SSVSA                                     *")
        print(UT.TS(), ": ", "*  -SSVQ                                      *")
        print(UT.TS(), ": ", "*  -duplicados                                *")
        self.TS=pd.to_datetime(self.TS)
        # Archivos a leer
        self.BD = pd.DataFrame()
    
    def cargadorBD(self, database):
        print(UT.TS(), ": ", "Cargando BD")
        self.BD = database
        print(UT.TS(), ": ", "BD Cargada")
    
    def FiltraGuarda60(self):
        print(UT.TS(),": ","Creando Base Corta (60 dias)...")
        bd_60 = dt.datetime.now() - dt.timedelta(days=60)
        self.index_60 = self.BD[self.BD.fecha_notificacion > pd.to_datetime(str(bd_60))].index
        UT.guardador(self.BD.loc[self.index_60],
                  "Valparaiso_notificaciones_2021_60_dias",
                  "csv"
                  )
        print(UT.TS(),": ","Base de 60 dias Exportada")
    
    def FiltraGuarda11(self):
        print(UT.TS(),": ","Creando BD 11 Dias...")
        bd_11 = dt.datetime.now() - dt.timedelta(days=11)
        self.index_11 = self.BD[self.BD.fecha_notificacion > pd.to_datetime(str(bd_11))].index
        UT.guardador(self.BD.loc[self.index_11],
                  "Valparaiso_notificaciones_2021_11_dias",
                  "csv"
                  )
        print(UT.TS(),": ","Base de 11 dias Exportada")
    
    def SSVSA(self):
        print(UT.TS(),": ",'Creando Base notificación SSVSA...')
        ssvsa_comunas = ["Algarrobo", 
                         "Cartagena", 
                         "Casablanca", 
                         "El Quisco", 
                         "El Tabo", 
                         "San Antonio", 
                         "Santo Domingo", 
                         "Valparaiso"
                         ]
        ssvsa_index = self.BD.loc[self.index_60][self.BD.loc[self.index_60]["comuna_residencia"].isin(ssvsa_comunas)].index
        UT.guardador(self.BD.loc[ssvsa_index],
                  "notificacionesssvsa",
                  "csv"
                  )
        print(UT.TS(),": ","base ssvsa lista")
    
    def SSVQ(self):
        print(UT.TS(),": ",'Creando Base notificación SSVQ...')
        ssvq_comunas = ["Cabildo", 
                        "Calera", 
                        "Con Con", 
                        "Hijuelas", 
                        "La Cruz", 
                        "La Ligua", 
                        "Limache", 
                        "Nogales", 
                        "Olmué", 
                        "Papudo", 
                        "Petorca", 
                        "Puchuncaví", 
                        "Quillota", 
                        "Quilpue", 
                        "Quintero", 
                        "Villa Alemana", 
                        "Viña del Mar", 
                        "Zapallar"
                        ]
        ssvq_index = self.BD.loc[self.index_60][self.BD.loc[self.index_60]["comuna_residencia"].isin(ssvq_comunas)].index
        UT.guardador(self.BD.loc[ssvq_index],
                  "notificacionesssvq",
                  "csv"
                  )
        print(UT.TS(),": ","base ssvq lista")
        
    def SSA(self):
        print(UT.TS(),": ",'Creando Base notificación SSA...')
        ssa_comunas= ["Calle Larga", 
                      "Catemu", 
                      "Llaillay", 
                      "Los Andes", 
                      "Panquehue", 
                      "Putaendo", 
                      "Rinconada", 
                      "San Esteban", 
                      "San Felipe", 
                      "Santa Maria"
                      ]
        ssa_index = self.BD.loc[self.index_60][self.BD.loc[self.index_60]["comuna_residencia"].isin(ssa_comunas)].index
        UT.guardador(self.BD.loc[ssa_index],
                  "notificacionesssa",
                  "csv"
                  )
        print(UT.TS(),": ","base ssa lista")
    
    def duplicados(self):
        print(UT.TS(),": ","Creando BD Duplicados...")
        print(UT.TS(),": ","Aplicando Filtros a Base...")
        # dejamos solo casos confirmados y probables, con notificaciones validadas e inconclusas desde 14 dias a la fecha
        etapa_clinica = ["PROBABLE","CONFIRMADA"]
        estado_notificacion = ["Validada", "Inconcluso"]
        
        bd_dupl_index = self.BD.loc[self.index_11][self.BD.loc[self.index_11]["etapa_clinica"].isin(etapa_clinica)].index
        bd_dupl_index = self.BD.loc[bd_dupl_index][self.BD.loc[bd_dupl_index]["estado_caso"].isin(estado_notificacion)].index
        print(UT.TS(),": ","Base Filtrada")
        
        print(UT.TS(),": ","Detectando Duplicados...")
        # encontramos los rut que estan duplicados, por ende tienen mas de una notificacion confirmada 
        #o probable en este periodo de tiempo
        repetido = []
        unico = []
        
        for x in self.BD.loc[bd_dupl_index]["identificacion_paciente"]:
        	if x not in unico:
        		unico.append(x)
        	else:
        		if x not in repetido:
        			repetido.append(x)
        
        print(UT.TS(),": ","Duplicados Cargados")
        print(UT.TS(),": ","Filtrando Duplicados...")
        
        # creamos una base nueva solo con los casos duplicados
        BD_dup_index = self.BD.loc[bd_dupl_index][self.BD.loc[bd_dupl_index]["identificacion_paciente"].isin(repetido)].index
        print(UT.TS(),": ",'BD Nueva creada, se ha de exportar.')
        print(UT.TS(),": ","Exportando Base...")
        UT.guardador(self.BD.loc[BD_dup_index],
                  "BASE DUPLICADOS VALPARAISO 2021",
                  "csv"
                  )
        print(UT.TS(),": ","Base Duplicados Exportada")
                        
