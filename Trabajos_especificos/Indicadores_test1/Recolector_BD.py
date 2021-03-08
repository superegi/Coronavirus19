
import pandas as pd
import os
import script_importador as importa
import datetime as dt
import compilador_epivigila as ce
import compilador_seguimiento as cs

import concurrent.futures
import concurrent
import threading
        
        
class Recolector_labs_seguimiento_epivigila:
    
    def __init__(self):
        import pandas as pd
        import os
        import script_importador as importa
        import datetime as dt
        import compilador_epivigila as ce
        import compilador_seguimiento as cs
        
        import concurrent.futures
        import concurrent
        import threading

        self.lista_fechas = []
        self.BD_labs = pd.DataFrame()
        print('Importado  modulo')
        
    # #
    # ###############################################
    # # Laboratorios
    # ###############################################
    # #
    
    def rango_fechas(self,fecha, dias_antes):
    	TS_analisis = pd.to_datetime(fecha)
    	self.lista_fechas = [fecha]
    	for contando_dias in range(1,dias_antes,1):
    		print(contando_dias)
    		self.lista_fechas.append(
                    (TS_analisis - dt.timedelta(days=contando_dias)).strftime('%Y%m%d'))
    	print(self.lista_fechas)
    	return self.lista_fechas
    
    def exec_laboratorios(self,fecha):
    	import compilador_labsFTP as clab
        
    	lee = importa.Importador_BASEDATOS()
    	lee.archivo_busca(direccion='../../BDs_FTP/')
    	lee.archivo_selecciona(fecha=fecha, area= 'Laboratorio_regiones')
    	lee.archivo_carga()
        
    	compila = clab.Compilador_laboratorios()
    	compila.cargadorBD(lee.BD)
    	compila.corrijo_rut()
    	compila.corrijo_resultadotest()
    	compila.agrego_nombreBD(str(fecha))
    	compila.set_index()
        
    	return compila.BD
    
    # #
    # ###############################################
    # # EPIVIGILA
    # ###############################################
    # #
    
    def exec_epivigila(self,fecha):
    
    	lee = importa.Importador_BASEDATOS()
    	lee.archivo_busca(direccion='../../BDs_FTP/Valparaíso')
    	lee.archivo_selecciona(fecha=fecha, area='notificaciones')
    	lee.archivo_carga()
    
    	compila = ce.Compilador_epivigila()
    	compila.cargadorBD(lee.BD)
    	compila.limpieza1()
    	compila.limpieza2()
    	compila.limpieza3()
    	print('Exportando')
    	compila.guardo('./EpiV.pkl')
    	print('exportado')
    
    # #
    # ###############################################
    # # Seguimiento
    # ###############################################
    # #
    
    def exec_seguimiento(self, fecha):
    
    	lee = importa.Importador_BASEDATOS()
    	lee.archivo_busca('../../BDs_FTP/Valparaíso')
    	lee.archivo_selecciona(fecha = fecha, area='Seguimiento')
    	lee.archivo_carga()
    
    	compila = cs.Compilador_seguimiento()
    	compila.cargadorBD(lee.BD)
    	compila.limpieza1()
    	compila.limpieza2()
    	compila.arreglo_folios()
    	compila.arreglo_BOOL()
    	compila.guardo('./Seguimiento.pkl')
    
    # #
    # ###############################################
    # # Ejecución
    # ###############################################
    # #
    
#    def TODOS_exec_laboratorios(lista_fechas):
#    	import concurrent.futures
#    	import requests
#    
#    	with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
#    		executor.map(exec_laboratorios, lista_fechas)
    
    def TODOS_exec_laboratorios2(self, lista_fechas):
    
    	with concurrent.futures.ThreadPoolExecutor() as executor:
    		futures = []
    		for fecha_x in lista_fechas:
    			futures.append(executor.submit(self.exec_laboratorios, fecha_x))
    		for future in concurrent.futures.as_completed(futures):
    			self.BD_labs = future.result()
    
    def main(self, fecha, dias_antes):
        
        
        lista_fechas = self.rango_fechas(fecha, dias_antes)
    
    
#    Corro el multithrething para los laboratorios
    
        self.TODOS_exec_laboratorios2(lista_fechas)
        print('Exportando laboratorios')
        self.BD_labs.to_pickle('./labs.pkl')
        print('exportado laboratorios')
    
    
# corro el multithrething para epivigila y seguimiento
    
        t1 = threading.Thread(target=self.exec_epivigila, args=(fecha,))
        t2 = threading.Thread(target=self.exec_seguimiento, args=(fecha,))
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        print('Completado!')
    

# if __name__ == "__main__":
#    extractor = Recolector_labs_seguimiento_epivigila()

#    extractor.main('20210228', 7)

    
