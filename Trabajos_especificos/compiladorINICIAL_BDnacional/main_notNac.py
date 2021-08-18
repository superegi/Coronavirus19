import divisor_notNac as dNac
import script_importador as si
import filtrador_notNac as fnotNac


UT=si.Utilitarios()

#NOTAR QUE EL CSV TIENE QUE LLEVAR POR NOMBRE 'export.csv' !!!

#Primero divide el .CSV gigante en partes mas pequeñas
#y las deja en formato .pkl que es mucho mas trabajable,
#gasta menos RAM, etc.
#Luego se pueden eliminar

#Creará partes de 500.000 entradas, las que se guardan en la carpeta
#donde se está trabajando

#La creación de los archivos .pkl solo debe ejecutarse 1 vez o siempre
#estará creando nuevos archivos cada vez que se ejecute el main

print(UT.TS(),": ",'INICIO DEL PROGRAMA FILTRADOR DE BD')

print(UT.TS(),": ",'a continuacion se particionará la BD original')
pkl=dNac.Divisor()
pkl.CargaData()
pkl.CreaPartes()
print(UT.TS(),": ",'Particiones formato .pkl creadas!')

print(UT.TS(),": ",'Ahora se buscaran y agregaran los archivos a una BD ')
print(UT.TS(),": ","Busco los archivos que dejaré como uno solo")
notNAC_archivos = si.Buscayagrega()

#Luego se ingresa la ruta y el formato de los archivos a juntar
#ojo que solo soporta archivos formato .pkl
#fue hecho para ser usado en conjunto con el divisor de antes
#Poner el nombre de la carpeta donde estan descargados los archivos de trabajo.
notNAC_archivos.busqueda_archivos(".", '.pkl')
# la siguiente agrega una columna extra con el nombre
# de la base de  y la compila
notNAC_archivos.compilador_basedatos(nombreBD=True)
print(notNAC_archivos.dataset.info)


# en la segunda parte la base con todos las partes unidas se filtra
#segun la cantidad de dias anteriores y el servicio de salud
BD = notNAC_archivos.dataset.copy()
print(UT.TS(),": ",'Se exporta BD notificaciones 2021...')
BD.to_csv("Valparaiso_notificaciones_2021" + ".csv", 
            sep=";",
            encoding='utf-8-sig', 
            index=False
            )
print(UT.TS(),": ",'Nueva BD exportada satisfactoriamente!')
print(UT.TS(),": ",'Comienza proceso de filtrado y exportado...')
filtrador=fnotNac.Filtrador()
filtrador.cargadorBD(BD)
filtrador.FiltraGuarda60()
filtrador.FiltraGuarda11()
filtrador.SSVSA()
filtrador.SSVQ()
filtrador.SSA()
filtrador.duplicados()

print(UT.TS(),": ",'FIN DEL PROGRAMA!!!')