import pandas as pd
import script_importador as si
import Compilador_PTNM as cPNTM

# importa muchos archivos de la PNTM| y los
# junta y compila
# para ello se usan dos scripts que están en la misma
# carpeta


# en la primera parte importo todos los archivos
# que son de la PNTM para juntarlos en uno solo

print("Busco los archivos que dejaré como uno solo")
PNTM_archivos = si.Buscayagrega()
# ojo como junciona la siguiente función
# lo que hace es hacer subsets con cada parámetro
PNTM_archivos.busqueda_archivos("./PNTM", ".xls")
# la siguiente agrega una columna extra con el nombre
# de la base de datos

#############################################################
#############################################################
# Mucho ojo con esto para que no se quede pegado!!!
# Ajusta los hilos a no más del 75% de lo que tengas disponible
# en tu PC
#############################################################

# SIN multiprocessing
# PNTM_archivos.compilador_basedatos(nombreBD=True)

# CON multiprocessing
PNTM_archivos.BD_juntador(numero_hilos=5, nombreBD=True)
#############################################################
#############################################################

# en la segunda parte la base con todos los archivos de
# la PNTM los compilo. arreglo fechas, junto nombres,....

BD = PNTM_archivos.dataset.copy()
compilador = cPNTM.Compilador()
print(BD.info())
compilador.cargadorBD(BD)
compilador.junta_nombre()
compilador.nuevasColumnas_TS()
compilador.a_TS()
compilador.otros_TS()
compilador.nuevasColumnas_DT()
compilador.dropearRepetidas()
compilador.reordenarColumnas()
compilador.normalizador_nombresColumnas()
print(compilador.BD)

# acá se eliminan repetidos. Dado que los archivos de la
# PNTM es en rango de fechas.... pasa que hay muchos valores
# repetidos, por lo que requiero eliminarlos y dejar el más nuevo


# elimino los repetidos, dejo el más nuevo
compilador.BD.sort_values(by="NombreBD", ascending=True, inplace=True)
compilador.BD.drop_duplicates(subset="idMuestra", keep="last", inplace=True)


# exporto el resultado
compilador.guardo_xls("PNTM_normalizada.xlsx")
compilador.guardo_pickle("PNTM_merged_formated.pkl")
print("Fin del script")
