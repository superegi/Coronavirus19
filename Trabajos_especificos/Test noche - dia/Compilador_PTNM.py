import pandas as pd
import datetime as dt
from itertools import cycle


class Compilador:
    def __init__(self):
        self.TS = dt.datetime.now().strftime("%m-%d %H:%M:%S")
        print(self.TS, ": ", "*** Inicio compilador de BD de PTNM      ***")
        print(self.TS, ": ", "*** Este objeto cuenta con las clases:   ***")
        print(self.TS, ": ", "*  -cargadorBD(ruta_BD)          *")
        print(self.TS, ": ", "*  -rutVerificador               *")
        print(self.TS, ": ", "*  -a_string                     *")
        print(self.TS, ": ", "*  -nuevasColumnas_TS            *")
        print(self.TS, ": ", "*  -a_TS                         *")
        print(self.TS, ": ", "*  -nuevasColumnas_DT            *")
        print(self.TS, ": ", "*  -dropearRepetidas             *")
        print(self.TS, ": ", "*  -reordenarColumnas            *")
        print(self.TS, ": ", "*  -normalizador                 *")
        print(self.TS, ": ", "*  -nombrarArchivo(ruta_BD)      *")
        print(self.TS, ": ", '*  -rutaGuardado(ruta_"".txt)    *')
        print(self.TS, ": ", "*  -guardarExcel                 *")
        print(self.TS, ": ", "*  -guardarPickle                *")
        # Archivos a leer
        self.BD = pd.DataFrame()

    def cargadorBD(self, database):
        print(self.TS, ": ", "Cargando BD")
        self.BD = database
        print(self.TS, ": ", "BD Cargada")

    def junta_nombre(self):
        self.BD["nombrePaciente"] = (
            self.BD.nombre_paciente
            + " "
            + self.BD.apellido_paterno_paciente
            + " "
            + self.BD.apellido_materno_paciente
        )

    def nuevasColumnas_TS(self):
        print(self.TS, ": ", "Junto fecha con hora")

        def juntador(DF, x, y):
            resultado = DF[x] + " " + DF[y]
            return resultado

        self.BD["TS_toma"] = juntador(self.BD, "fecha_toma_muestra", "hora_muestra")
        self.BD["TS_recepcion"] = juntador(
            self.BD, "fecha_recepcion_muestra", "hora_recepcion"
        )
        self.BD["TS_resultado"] = juntador(
            self.BD, "fecha_resultado_muestra", "hora_resultado"
        )
        self.BD["TS_recepcion_i"] = juntador(
            self.BD,
            "fecha_informada_recepcion_laboratorio",
            "hora_informada_recepcion_laboratorio",
        )
        self.BD["TS_resultado_i"] = juntador(
            self.BD,
            "fecha_informada_resultado_laboratorio",
            "hora_informada_resultado_laboratorio",
        )

    def a_TS(self):
        print(self.TS, ": ", "Transformo texto a TS (timestamp)")
        cols_BD = [f for f in list(self.BD.columns) if "TS_" in f]
        for i in cols_BD:
            print("Transformando: ", i)
            self.BD[i] = pd.to_datetime(
                self.BD[i], errors="coerce", format="%d-%m-%Y %H:%M:%S"
            )
        # Se le quita la zona horaria a la columna 'TS_fecha_creacion

    def otros_TS(self):
        self.BD["TS_fecha_creacion"] = self.BD["fecha_creacion"]
        self.BD["TS_fecha_creacion"] = pd.to_datetime(
            self.BD["TS_fecha_creacion"], errors="coerce", dayfirst=True
        )
        self.BD["TS_fecha_creacion"] = self.BD["TS_fecha_creacion"].dt.tz_localize(None)

        self.BD["TS_nacimiento_pac"] = pd.to_datetime(
            self.BD["fecha_nacimiento_paciente"], dayfirst=True, errors="coerce"
        )

    def nuevasColumnas_DT(self):
        print(self.TS, ": ", "Creo DT (deltatime)")

        def restador(DF, x, y):
            resultado = DF[x] - DF[y]
            return resultado

        self.BD["DT_llegaMuestra"] = restador(self.BD, "TS_recepcion", "TS_toma")
        self.BD["DT_procesaMuestra"] = restador(self.BD, "TS_resultado", "TS_recepcion")
        self.BD["DT_procesaMuestra_i"] = restador(
            self.BD, "TS_resultado_i", "TS_recepcion_i"
        )

    def dropearRepetidas(self):
        print(self.TS, ": ", "Elimino columnas inutiles")
        self.BD.drop(
            columns=[
                "nombre_profesional",
                "rut_profesional",
                "titulo_profesional",
                "especialidad_profesional",
                "medico_solicitante",
                "rut_medico_solicitante",
                "fecha_toma_muestra",
                "hora_muestra",
                "fecha_recepcion_muestra",
                "hora_recepcion",
                "fecha_resultado_muestra",
                "hora_resultado",
                "nombre_paciente",
                "apellido_paterno_paciente",
                "apellido_materno_paciente",
                "estado_muestra_significado",
                "fecha_informada_recepcion_laboratorio",
                "hora_informada_recepcion_laboratorio",
                "fecha_informada_resultado_laboratorio",
                "hora_informada_resultado_laboratorio",
                "fecha_nacimiento_paciente",
                "fecha_creacion",
            ],
            index=1,
            inplace=True,
        )

    def reordenarColumnas(self):
        columnas = [
            "id_paciente",
            "tipo_documento_paciente",
            "pais_origen_paciente",
            "nombrePaciente",
            "TS_nacimiento_pac",
            "edad_paciente",
            "comuna_paciente",
            "dirección_paciente",
            "telefono_paciente",
            "paciente_email",
            "sexo_paciente",
            "prevision_paciente",
            "id_muestra",
            "estado_muestra",
            "tipo_muestra",
            "resultado",
            "TS_toma",
            "TS_recepcion",
            "TS_resultado",
            "DT_llegaMuestra",
            "DT_procesaMuestra",
            "TS_recepcion_i",
            "TS_resultado_i",
            "DT_procesaMuestra_i",
            "TS_fecha_creacion",
            "codigo_muestra_cliente",
            "laboratorio",
            "tipo_laboratorio",
            "codigo_servicio_salud_laboratorio",
            "servicio_salud_laboratorio",
            "establecimiento",
            "tecnica_muestra",
            "codigo_servicio_salud_establecimiento",
            "servicio_salud_establecimiento",
            "epivigila",
            "busqueda_activa",
            "tiposolicitud",
            "codigo_muestra_cliente",
        ]

        if any([e for e in self.BD.columns if "NombreBD" in e]):
            x = list(columnas) + list(["NombreBD"])
            self.BD = self.BD[x]
        else:
            self.BD = self.BD[columnas]

    def normalizador_nombresColumnas(self):
        normalizacion_cols = dict(
            {
                "correlativo SEREMI": "ID_SEREMI",
                "nombrePaciente": "Nombre",
                "id_paciente": "RUT",
                "edad_paciente": "Edad",
                "sexo_paciente": "Sexo",
                "tipo_muestra": "tipoMuestra",
                "TS_toma": "TS_tomaMuestra",
                "TS_recepcion": "TS_recepcionMuestra",
                "TS_resultado": "TS_resultadoMuestra",
                "busqueda_activa": "busquedaActiva",
                "tiposolicitud": "tipoSolicitud",
                "laboratorio": "Laboratorio",
                "Región de laboratorio donde se procesa la muestra": "LaboratorioRegion",
                "telefono_paciente": "telefonoPaciente",
                "paciente_email": "mailPaciente",
                "dirección_paciente": "direccionPaciente",
                "tipo_documento_paciente": "identificacionPaciente",
                "pais_origen_paciente": "paisPaciente",
                "TS_nacimiento_pac": "TS_nacimientoPaciente",
                "comuna_paciente": "comunaPaciente",
                "dirección_paciente": "direccionPaciente",
                "prevision_paciente": "previsionPaciente",
                "id_muestra": "idMuestra",
                "estado_muestra": "estadoMuestra",
                "resultado": "resultadoMuestra",
                "TS_recepcion_i": "TS_recepcionInformada",
                "TS_resultado_i": "TS_resultadoInformado",
                "TS_fecha_creacion": "TS_creacionRegistro",
                "codigo_muestra_cliente": "idmuestraCliente",
                "tipo_laboratorio": "tipoLaboratorio",
                "codigo_servicio_salud_laboratorio": "codssLaboratorio",
                "servicio_salud_laboratorio": "ssLaboratorio",
                "establecimiento": "establecimiento",
                "tecnica_muestra": "tecnicaMuestra",
                "codigo_servicio_salud_establecimiento": "codssEstablecimiento",
                "servicio_salud_establecimiento": "ssEstablecimiento",
                "epivigila": "epivigilia",
            }
        )
        # Reasigno nombres
        self.BD = self.BD.rename(columns=normalizacion_cols)

    def guardo_xls(self, ruta):
        print(self.TS, ": ", "Guardando archivo Excel")
        self.BD.to_excel(ruta)
        print(self.TS, ": ", "Guardado! Excel")

    def guardo_pickle(self, ruta):
        print(self.TS, ": ", "Guardando archivo pickle")
        self.BD.to_pickle(ruta)
        print(self.TS, ": ", "Guardado!")
