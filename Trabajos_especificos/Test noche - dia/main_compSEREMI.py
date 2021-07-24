# main del compilador del CONSOLIDADO
# de la seremi de salud

import pandas as pd
import Compilador_compSEREMI as cSEREMI


CS = cSEREMI.Compilador_compiladoSEREMI()
CS.cargadorBD(pd.read_csv("./Seremi/Julioseremi.csv", sep=";"))
CS.corrijo_rut()
CS.corrijo_resultadotest()
CS.corrijo_fechas()
CS.corrijo_sexo()
CS.arreglo_edad()

# CS.BD.to_excel("Seremi_compilado.xlsx")

CS.elimino_columnas()
CS.elimino_columnas2()
CS.BD.head(1000).to_excel("Seremiejemplo.xlsx")
CS.BD.to_pickle("SEREMI_compilado.pkl")
