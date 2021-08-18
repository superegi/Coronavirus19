import pandas as pd
import numpy as np
import datetime as dt
import os
import matplotlib.pyplot as plt


# from IPython.core.interactiveshell import InteractiveShell

# InteractiveShell.ast_node_interactivity = "all"

seremi = pd.read_pickle("./SEREMI_compilado.pkl")
seremi.tail(2)
seremi.shape
seremi.columns

# tengo que sumer 1, dado que esa columna está atrada 24 horas (1 día)
seremi.TS_valida = seremi.TS_valida + dt.timedelta(days=1)

pntm = pd.read_pickle("./PNTM_merged_formated.pkl")
pntm.tail(2)
pntm.columns

FII = [
    pd.to_datetime("2021-07-06", dayfirst=False),
    pd.to_datetime("2021-07-25", dayfirst=False),
]
rango_s = (seremi.TS_recepcion > FII[0]) & (seremi.TS_recepcion < FII[1])
rango_s.value_counts()
seremi[rango_s].TS_recepcion.hist()

FII = [
    pd.to_datetime("2021-07-01", dayfirst=False),
    pd.to_datetime("2021-07-30", dayfirst=False),
]
rango_p = (pntm.TS_creacionRegistro > FII[0]) & (pntm.TS_creacionRegistro < FII[1])
pntm[rango_p].TS_creacionRegistro.hist()

print("cuantos de seremi se encuentran en pntm")
print(seremi[rango_s].id_paciente.isin(pntm[rango_p].id_paciente).value_counts())
print(
    seremi[rango_s]
    .id_paciente.isin(pntm[rango_p].id_paciente)
    .value_counts(normalize=True)
)
print("cuantos de pntm se encuentran en seremi")
print(pntm[rango_p].id_paciente.isin(seremi[rango_s].id_paciente).value_counts())
print(
    pntm[rango_p]
    .id_paciente.isin(seremi[rango_s].id_paciente)
    .value_counts(normalize=True)
)
print()

####################################################
# Lo más importante
###################################################
no_aparecen = seremi[rango_s][
    seremi[rango_s].id_paciente.isin(pntm[rango_p].id_paciente) == False
]
# no_aparecen[['Nombre', 'id_paciente', 'TS_valida']].to_excel('NO ESTAN.xlsx')
####################################################

print(seremi.shape)
print(no_aparecen.shape)
print((no_aparecen.shape[0] / seremi.shape[0]) * 100)
print()


no_aparecen.columns
print(
    no_aparecen["Hospital o establecimiento de origen (lugar donde se toma la muestra)"]
    .value_counts()
    .head(10)
)
print(
    no_aparecen["Hospital o establecimiento de origen (lugar donde se toma la muestra)"]
    .value_counts(normalize=True)
    .head(10)
    * 100
)
print(
    no_aparecen["Laboratorio de referencia (lugar donde se procesa la muestra)"]
    .value_counts()
    .head(10)
)
print(
    no_aparecen["Laboratorio de referencia (lugar donde se procesa la muestra)"]
    .value_counts(normalize=True)
    .head(10)
    * 100
)

####################################################
# Lo más importante
###################################################
si_aparecen = seremi[rango_s][
    seremi[rango_s].id_paciente.isin(pntm[rango_p].id_paciente) == True
]
###################################################

interes = [
    "id_paciente",
    "Nombre",
    "Hospital o establecimiento de origen (lugar donde se toma la muestra)",
    "Región de establecimiento de origen",
    "Laboratorio de referencia (lugar donde se procesa la muestra)",
    "Región de laboratorio donde se procesa la muestra",
    "Verificador_RUT",
    "RESULT",
    "TS_toma",
    "TS_recepcion",
    "TS_resultado",
    "TS_valida",
    "SEXO",
    "EDAD",
]
si_aparecen = si_aparecen[interes]

####################################################
# Lo más importante
###################################################
cruzados = pd.merge(si_aparecen, pntm, on="id_paciente")
cruzados.columns
###################################################

# herramienta para calcular muchas cosas de cada indicador
def carculador(
    df,
    tiempo1,
    tiempo2,
    diferencial,
    titulo,
    xlabel="Cantidad de días",
    ylabel="Frecuencia relativa",
):
    df[diferencial] = df[tiempo2] - df[tiempo1]
    BD = df.copy()

    BD = BD.loc[
        (BD[diferencial] < BD[diferencial].quantile(0.98))
        & (BD[diferencial] > BD[diferencial].quantile(0.02))
    ]
    x = BD[[tiempo1, tiempo2, diferencial]].sample(5)
    print(x)

    print(BD[diferencial].describe())

    BD["temp"] = BD[diferencial].dt.days + BD[diferencial].dt.seconds / (3600 * 60)
    BD.temp.hist(density=True, histtype="stepfilled")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(titulo)


carculador(
    cruzados,
    "TS_toma",
    "TS_tomaMuestra",
    "DT_tomamuestra",
    "Histograma distribución diferencia \n Toma de muestra",
)

carculador(
    cruzados,
    "TS_recepcion",
    "TS_recepcionMuestra",
    "DT_recepcion",
    "Histograma distribución diferencia \n Recepción de muestra",
)

carculador(
    cruzados,
    "TS_resultado",
    "TS_resultadoMuestra",
    "DT_resultado",
    "Histograma distribución diferencia \n Recepción de muestra",
)

carculador(
    cruzados,
    "TS_valida",
    "TS_resultadoMuestra",
    "DT_disponible",
    "Histograma distribución diferencia \n Recepción de muestra",
)


# Dado que baje varias bases de datos, debo 'disponibilizar'
#  según la hora de bajada
#
# creo una variable TS desde el nombre de la BD
cruzados["TS_BD"] = cruzados.NombreBD.apply(
    lambda x: pd.to_datetime(x.split(" ")[-2], dayfirst=False, yearfirst=True)
)

# según si la base es AM o PM le agrego horas
AM = cruzados.NombreBD.str.contains("AM")
PM = cruzados.NombreBD.str.contains("PM")
cruzados.loc[AM, "TS_BD"] = cruzados.loc[AM].TS_BD + dt.timedelta(hours=8)
cruzados.loc[PM, "TS_BD"] = cruzados.loc[PM].TS_BD + dt.timedelta(hours=12)

carculador(
    cruzados,
    "TS_valida",
    "TS_BD",
    "DT_disponible2",
    "Histograma distribución diferencia \n Recepción de muestra",
)


# epidemiologia
print(seremi.EDAD.describe())
print(seremi.SEXO.value_counts())
print(seremi.SEXO.value_counts(normalize=True))
print(seremi.RESULT.value_counts())
print(seremi.RESULT.value_counts(normalize=True))

pntm.Edad = pd.to_numeric(pntm.Edad)
print(pntm.Edad.describe())
print(pntm.Sexo.value_counts())
print(pntm.Sexo.value_counts(normalize=True))
print(pntm.resultadoMuestra.value_counts())
print(pntm.resultadoMuestra.value_counts(normalize=True))
