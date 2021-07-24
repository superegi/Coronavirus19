# -*- coding: utf-8 -*-
"""
Editor de Spyder

Este es un archivo temporal.
"""

import pandas as pd
import numpy as np
import datetime as dt
import os

from itertools import cycle

# from funciones_auxilares import pcolor


class Compilador_compiladoSEREMI:
    def __init__(self):
        print("Inicio módulo de compilación y limpieza")
        print("Compilado nocturno SEREMI")
        print("Las funciones son:")
        print("cargadorBD")
        print("corrijo_rut")
        print("corrijo fechas")
        print("guardo")
		
        # genero una marca de tiempo
        self.TS = dt.datetime.now().strftime("%m-%d %H:%M:%S")


        # Archivos a leer
        self.BD = pd.DataFrame()

    def cargadorBD(self, database):
        self.BD = database

    def digito_verificador(self, rut):
        reversed_digits = map(int, reversed(str(rut)))
        factors = cycle(range(2, 8))
        s = sum(d * f for d, f in zip(reversed_digits, factors))
        if (-s) % 11 == 10:
            return "K"
        else:
            return (-s) % 11

    def verifico_RUT(self, valor):
        try:
            RUT_i = str(valor)[:-1]
            RUT_DV = str(valor)[-1]
            if (str(self.digito_verificador(RUT_i)) == RUT_DV) == True:
                return "RUT_OK"
            else:
                return "RUT_error"
        except:
            print("ERROR! ERROR! ERROR!")
            print(valor)
            return "RUT_error"

    # esta es una sub_funcion
    def agrego_palitoRUT(self):
        a = self.BD.Verificador_RUT == "RUT_OK"
        RUUUUTT = self.BD.RUN.str[:-1] + "-" + self.BD.RUN.str[-1:]
        self.BD.loc[a == True, "RUT"] = RUUUUTT

    def corrijo_rut(self):
        print(self.TS, ": ", "Corrijo RUTs")
        self.BD["Verificador_RUT"] = self.BD.RUN.apply(lambda x: self.verifico_RUT(x))
        self.agrego_palitoRUT()
        print(self.TS, ": ", "finalizado corrección RUTs")

    # no usado aca........
    def corrijo_resultadotest(self):
        self.BD.loc[self.BD.Resultado.isna() == True, "RESULT"] = np.nan

        # los negativos
        self.BD.loc[
            self.BD.Resultado.str.contains("nega", case=False, na=False), "RESULT"
        ] = "Negativo"

        self.BD.loc[
            self.BD.Resultado.str.contains("NEGATIVO", case=False, na=False), "RESULT"
        ] = "Negativo"

        # los positivos
        self.BD.loc[
            self.BD.Resultado.str.contains("posi", case=False, na=False), "RESULT"
        ] = "Positivo"

        # Los descartados
        dum = ["MUESTRA NO APTA"]
        for x in dum:
            self.BD.loc[
                self.BD.Resultado.str.contains(x, case=False, na=False), "RESULT"
            ] = "Descartada"
        # Los indeterminados

        dum = ["indeter"]
        for x in dum:
            self.BD.loc[
                self.BD.Resultado.str.contains(x, case=False, na=False), "RESULT"
            ] = "Indeterminado"

        # Los nueva muestra
        dum = ["nueva muestra"]
        for x in dum:
            self.BD.loc[
                self.BD.Resultado.str.contains(x, case=False, na=False), "RESULT"
            ] = "Requiere nueva muestra"

    def corrijo_fechas(self):
        self.BD["TS_toma"] = pd.to_datetime(
            self.BD["Fecha de toma de muestra"],
            dayfirst=True,
            yearfirst=False,
            errors="coerce",
        )

        self.BD["TS_recepcion"] = pd.to_datetime(
            self.BD["Fecha de recepción de la muestra"],
            dayfirst=True,
            yearfirst=False,
            errors="coerce",
        )

        self.BD["TS_resultado"] = pd.to_datetime(
            self.BD["Fecha de resultado"],
            dayfirst=True,
            yearfirst=False,
            errors="coerce",
        )

        self.BD["TS_valida"] = pd.to_datetime(
            self.BD["fecha validación"],
            dayfirst=True,
            yearfirst=False,
            errors="coerce",
        )

    # no usado aca.......
    def agrego_nombreBD(self, nombre_BD):
        self.BD["nombre_BD"] = str(nombre_BD)

    def arreglo_edad(self):
        self.BD["EDAD"] = self.BD.Edad.str.split(" ", expand=True)[0]
        self.BD["EDAD"] = pd.to_numeric(self.BD["EDAD"], errors="coerce")

    def corrijo_sexo(self):

        self.BD.loc[
            self.BD.Sexo.str.contains("mas", case=False, na=False), "SEXO"
        ] = "Hombre"

        self.BD.loc[
            self.BD.Sexo.str.contains("hom", case=False, na=False), "SEXO"
        ] = "Hombre"

        self.BD.loc[
            self.BD.Sexo.str.contains("fem", case=False, na=False), "SEXO"
        ] = "Mujer"

        self.BD.loc[
            self.BD.Sexo.str.contains("muj", case=False, na=False), "SEXO"
        ] = "Mujer"

    def elimino_columnas(self):
        lista = [
            "Fecha de resultado",
            "Fecha de recepción de la muestra",
            "Fecha de toma de muestra",
            "fecha validación",
        ]

        self.BD.drop(lista, axis=1, inplace=True)

    def elimino_columnas2(self):
        lista = ["RUN", "Resultado", "Edad", "Sexo"]
        self.BD.drop(lista, axis=1, inplace=True)
        
    def guardo_xls(self, ruta ):
        print(self.TS, ": ", "Guardando BD como excel")
        self.BD.to_excel(ruta)
        print(self.TS, ": ", "Guardado BD!")
        
    def guardo_xls_ejemplo(self, ruta):
        print(self.TS, ": ", "Guardando BD como excel")
        self.BD.head(1000).to_excel(ruta)
        print(self.TS, ": ", "Guardado BD!")

    def guardo_pkl(self, ruta):
        print(self.TS, ": ", "Guardando BD como pickle")
        self.BD.to_pickle(ruta)
        print(self.TS, ": ", "Guardado BD!")

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
