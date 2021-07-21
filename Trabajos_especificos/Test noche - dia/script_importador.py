# -*- coding: utf-8 -*-
"""
Editor de Spyder

Este es un archivo temporal.
"""

import pandas as pd
import numpy as np
from pandas_ods_reader import read_ods
import os
import datetime as dt
from itertools import cycle


class Importador_BASEDATOS:
    def __init__(self):
        print("Inicio módulo de importación")

        print("Las funciones son:")
        print("archivo_busca")
        print("archivo_selecciona")
        print("archivo_carga")

        # Archivos a leer
        self.archivos = []
        self.archivo_seleccionado = []

        self.BD = pd.DataFrame()

    def archivo_busca(self, direccion=None):
        # Me aseguro que la dirección esté definida
        if direccion:
            ruta = direccion
        else:
            ruta = "../BDs/"

        print("Inicio lectura de archivos")
        lista_archivos = []
        for dirname, dirnames, filenames in os.walk(ruta):
            for filename in filenames:
                lista_archivos.append(os.path.join(dirname, filename))
        self.archivos = lista_archivos
        print(self.archivos)
        print("\n")

    def archivo_selecciona(self, fecha=None, area=None, tipo=None):
        """dentro del universo de los archivos encontrados
        selecciona a una submuestra definida por los argumentos
        FECHA, AREA y TIPO.
        Devuelve una lista que idealmente es sólo un archivo"""

        files = self.archivos

        # elimino archivos temporales
        elimina = [f for f in files if "~lock" in f]
        if elimina:
            for x in elimina:
                print("eliminando:", x)
                files.remove(x)

        # las reglas de subset
        if fecha:
            files = [f for f in files if fecha in f]
        if area:
            files = [f for f in files if area in f]
        if tipo:
            files = [f for f in files if tipo in f]

        print(files)
        self.archivo_seleccionado = files

    # Esto no es necesario si consigo meter el 'archivo_seleccionado'
    # en en proximo paso
    def archivo_carga(self, archivo=None):
        if archivo:
            file = archivo
        else:
            file = self.archivo_seleccionado[0]
        print(file)

        # me aseguro que tenga extension csv
        if any([e for e in [file] if "csv" in e]):
            self.BD = pd.read_csv(
                file,
                sep=";",
                # encoding= 'latin',
                # encoding='utf-8-sig',
                dayfirst=False,
                dtype=str,
            )

        # o quizás es excell
        if any([e for e in [file] if "xlsx" in e]):
            self.BD = pd.read_excel(file)


class Buscayagrega:
    def __init__(self):
        print("Clase compiladora de TABLA REM")
        # Creo algunas listas de variables interesantes

        # self.dataset con la que trabajaré
        self.dataset = pd.DataFrame()

    def busqueda_archivos(self, directorio, extension, clave=None):
        # Busco las bases de datos en los archivos
        # debo definir las variables 'directorio',
        # la variable 'extensión'
        # y la variable 'clave'

        print("Inicio búsqueda de los archivos a compilar")

        datos = []
        for dirname, dirnames, filenames in os.walk(directorio):
            for filename in filenames:
                datos.append(os.path.join(dirname, filename))

        # Subset
        if clave == None:
            self.archivos_raw = [f for f in datos if extension in f]
        else:
            primer_paso = [f for f in datos if clave in f]
            self.archivos_raw = [f for f in primer_paso if extension in f]

        print(self.archivos_raw)  # los listo

    def compilador_basedatos(self):
        # ==================================================
        # Compilo las bases de datos
        # ==================================================
        print("Compilo las bases de datos en un solo archivo")
        for f in self.archivos_raw:
            print("leyendo:", f)
            data = pd.read_excel(f, dtype=str)
            self.dataset = self.dataset.append(data)

        print("Base de datos resultado:")
        print(self.dataset.info())

    def guardo(self, ruta):
        print("Guardando como:", ruta)
        self.dataset.to_pickle(ruta)
        print("Guardado!")
