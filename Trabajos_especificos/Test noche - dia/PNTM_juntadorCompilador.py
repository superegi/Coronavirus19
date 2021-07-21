import pandas as pd
import script_importador as si
import compilador_PTNM as cPNTM


print("Busco los archivos que dejaré como uno solo")
PNTM_archivos = si.Buscayagrega()
# ojo como junciona la siguiente función
# lo que hace es hacer subsets con cada parámetro
PNTM_archivos.busqueda_archivos("./PNTM", ".xls", "20210720")
PNTM_archivos.compilador_basedatos()
print(PNTM_archivos.dataset.info)
PNTM_archivos.guardo("PNTM_compilado.pkl")


BD = pd.read_pickle("./PNTM_compilado.pkl")
compilador = cPNTM.Compilador()
print(BD.info())
compilador.cargadorBD(BD)
compilador.junta_nombre()
compilador.nuevasColumnas_TS()
compilador.a_TS()
compilador.fechaCreacion_TS()
compilador.nuevasColumnas_DT()
compilador.dropearRepetidas()
compilador.reordenarColumnas()
compilador.normalizador_nombresColumnas()
print(compilador.df)
compilador.guardo_xls("p_normalizada.xlsx")
compilador.guardo_pickle("PNTM_merged_formated.pkl")
