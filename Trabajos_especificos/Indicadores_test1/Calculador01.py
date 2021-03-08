import pandas as pd
import datetime as dt

def separador():
    print('-'*20)


# cargo las bases de datos previamente compiladas
EpiV = pd.read_pickle('./EpiV.pkl')
LabsN = pd.read_pickle('./labs.pkl')
Seguimiento =  pd.read_pickle('./Seguimiento.pkl')

# seteo el rango de fechas de interés en las
# notificaciones de epivigila
fecha1 = pd.to_datetime('2021-02-22')
fecha2 = pd.to_datetime('2021-02-28')

# y hago el subset
#Epi = EpiV.loc[(EpiV.fecha_notificacion > fecha1) & 
#                (EpiV.fecha_notificacion < fecha2)]

Epi = EpiV

#del EpiV


# selecciono las etapas de Epi que son 
# de interés para el análisis
etapa = ['CONFIRMADA', 'PROBABLE', 'SOSPECHA', 'BUSQUEDA A', 'DESCARTADA']
Epi = Epi.loc[Epi.etapa_clinica.isin(etapa)]

# ordeno la BD en orden
#
# ojo acá, tengo que saber que quiero eliminar de los duplicados
#

Epi['id'] = Epi.index
Epi.sort_values('id').drop_duplicates(
        'RUT',
        keep='last',
        inplace=True)
print(Epi[['RUT', 'fecha_notificacion', 'id']])
separador()

# hago el subset ahora de los laboratorios
labs = LabsN.loc[(LabsN.Fechatomademuestra > fecha1) & 
                (LabsN.Fechatomademuestra < fecha2)]

del LabsN
print('La base de datos tiene estos resultado de test')
print('distintos a los valores parametrizados')

print(labs.loc[labs.RESULT.isna() == True].Resultado.value_counts())
print('La base de datos tiene estos resultado de test')
print(labs.RESULT.value_counts())
separador()


lab_interes = labs[['RUT', 'RESULT', 'Fechatomademuestra', 'FechadeResultado']]

# Elimino RUT duplicados y me quedo con la última fecha
#
#
#   CONSULTAR!!!!!

lab_interes.sort_values('FechadeResultado').drop_duplicates(
        'RUT',
        keep = 'last',
        inplace = True)


##
## Genero el producto 1
##
## Es la unicón del resultado de laboratorio más
## actalizado. Se adosa a la base de notificaciones de epivigila
## para poder tener para cada notificación
## el resultado de laboratorio más actualizado
producto1 = pd.merge(Epi.reset_index(), lab_interes, on='RUT')
producto1 = pd.merge(Epi.reset_index(), lab_interes, on='RUT', how = 'left')
producto1.set_index('id_formulario_eno', inplace=True)

# Actualizo el 'estado del caso' de EPIVIGILA
#  dado que tengo el último estado del 
# resultado de laboratorio
'''
si el rut encontrado tiene palgún resultado....
    positivo: cambiar etapa clinica a confirmado
    negativo: cambiar etapa clinica a descartado
    
'''

id_positivos = producto1.loc[producto1.RESULT == 'Positivo'].index
id_negativos = producto1.loc[producto1.RESULT == 'Negativo'].index

pos_antes = producto1.loc[id_positivos]['etapa_clinica'].value_counts()
neg_antes = producto1.loc[id_negativos]['etapa_clinica'].value_counts()

producto1.loc[id_positivos, 'etapa_clinica'] = 'CONFIRMADA'
producto1.loc[id_negativos, 'etapa_clinica'] = 'DESCARTADA'

pos_dp = producto1.loc[id_positivos]['etapa_clinica'].value_counts()
neg_dp = producto1.loc[id_negativos]['etapa_clinica'].value_counts()

# imprimo el resultado de lo que estoy haciendo
Result_intermedio = pd.DataFrame(data = [pos_antes, pos_dp]).T
Result_intermedio.columns = ['Antes', 'Despues']
print('Cambio de etapa clínica en ', 'positivos')
print(Result_intermedio)

Result_intermedio = pd.DataFrame(data = [neg_antes, neg_dp]).T
Result_intermedio.columns = ['Antes', 'Despues']
print('Cambio de etapa clínica en ', 'negativos')
print(Result_intermedio)

Result_intermedio = pd.DataFrame(
        data = [
                Epi.etapa_clinica.value_counts(),
                producto1.etapa_clinica.value_counts()
                ]).T
Result_intermedio.columns = ['Antes', 'Despues']
Result_intermedio['Diferencia'] = Result_intermedio['Antes'] - Result_intermedio['Despues']

print('Cambio de etapa clínica en ', 'TODO')
print(Result_intermedio)



##
## Cambiar algo más del epivigila?????
# CONSULTAR!!!!!!
##

# Genero el producto2
#
# Es la junta de todos los casos de la base de datos
# de notificaciones con los resultados de laboratorio 
# actualizados y además con su estado de caso también 
# actualizado

producto2 = pd.merge(
    Seguimiento,
    producto1,
    right_on= 'numero_folio',
    left_on= 'FOLIO',
    how = 'left')

'''

prodcto2:

subset (tipo_seguimiento == 'caso')

calcular FECHE INICIO SEGUIMIENTO:
    
    CONFIMADOS:
        fecha resultado (del producto1)
        si no se encuentra.....
        SEGUIMIENTO.['CUMPLEREQUISITO']
    PROBABLSE:
        fecha de notificación (de seguimiento)
'''
# Caluclo la fecha de inicio de seguimiento
# para los CONFIRMADOS y los PROBABLES

producto2.loc[
        producto2.etapa_clinica == 'CONFIRMADA',
        'TS_inicio_seguimiento'] = producto2['FechadeResultado']

producto2.loc[
        (producto2.etapa_clinica == 'CONFIRMADA') &
        (producto2.TS_inicio_seguimiento.isnull() == True),
        'TS_inicio_seguimiento'] = producto2['fecha_cumple_requisitos']

producto2.loc[
        producto2.etapa_clinica == 'PROBABLE',
        'TS_inicio_seguimiento'] = producto2['fecha_notificacion_x']

# Caluclo la diff para el contacto telefónico

'''
calcular diff_contacto_telefono:
    TS INICIO SEGUIMIENTO - TS (dia contacto)
'''
producto2['diff_contacto_telefono'] = producto2.dia_contacto - producto2.TS_inicio_seguimiento

''' 
filtro
    6) Filtrar de acuerdo a las siguientes restricciones:
-etapa_clinica: CONFIRMADA, PROBABLE
-estado_paciente  (V) : vivo
-estado_caso (EO): validada, inconcluso
-vigente_no_eliminado (ES): t
-lugar_reposo (GY): domicilio particular, residencia sanitaria

Preparación base seguimiento
    1) Separar los datos en caso y contacto. Filtrar tipo_seguimiento en caso,
    y estos datos copiarlos y pegarlos en nueva hoja llamada CI.
    Filtrar tipo_seguimiento en contacto, y estos datos copiarlos y
    pegarlos en nueva hoja llamada CE. 
    2) Recuperar los datos de etapa clínica y fecha de resultado
    desde hoja denominadores de base notificación. Se realiza un
    buscar v por folio para así cruzar estos datos. En la columna
    fecha de resultado, reemplazar #N/D por vacío.
    3) Filtrar por etapa_clinica: confirmado y probable. Copiar
    y pegar en nueva hoja llamada confprob.
'''

producto2

P2 = producto2.loc[
        (producto2.etapa_clinica.str.contains('CONFIRMADA')) |
        (producto2.etapa_clinica.str.contains('PROBABLE'))]

P2 = P2.loc[P2.estado_paciente == 'vivo']

P2 = P2.loc[
        (P2.estado_caso.str.contains('Validada')) |
        (P2.estado_caso.str.contains('Inconcluso'))]

P2 = P2.loc[
        (P2.vigente_no_eliminado == 't')]

P2 = P2.loc[
        (P2.lugar_reposo.str.contains('Domicilio particular')) |
        (P2.lugar_reposo.str.contains('Residencia sanitaria'))]

P2 = P2.loc[
        (P2.tipo_seguimiento == 'caso')]


'''
indicador:
    para cada entrada:
        si (diff_contacto_telefono <= 3 dias) &
        (contacto_localizado == True): chachan!!!!
    
    filltrar indicador 5 paravspor cada FOLIO

indicador = ind5 == true / unique(folio)
            93 DS 0.5
'''

P2.loc[
    (P2.diff_contacto_telefono <= dt.timedelta(days=3)) &
    (P2.contacto_localizado == 'Si'),
    'i5'] = 'Cumplido'
       
       
       
P2.loc[
    (P2.i5.isna() == True) &
    ((P2.diff_contacto_telefono > dt.timedelta(days=3)) |
    (P2.contacto_localizado == 'No')),
    'i5'] = 'Incumplido'
        
P2 = P2[['FOLIO', 'FOLIO_CONTACTO', 'fecha_notificacion_x',
           'tipo_seguimiento', 'dia_contacto',
             'cont_fecha_seguimiento', 'contacto_localizado',
             'fecha_cumple_requisitos', 'etapa_clinica', 
             'RUT_x', 'Fechatomademuestra', 'FechadeResultado',
             'RESULT', 'tiene_resultado_covid',

             'TS_inicio_seguimiento', 'diff_contacto_telefono',
             'i5'
             
             ]]



separador()
print('Cantidad de folios')
ex = P2.FOLIO.nunique()
print(ex)
print()

print('cantidad de cumplidos totales')
ex = P2.i5.value_counts()
print(ex)
print()

print('Cumplidos al menos uno por folio')
dum = P2.groupby('FOLIO').i5.count().value_counts()
print(dum.loc[dum.index!=0].sum())

print('Indicador')
ex = dum.loc[dum.index!=0].sum()/P2.FOLIO.nunique()
print(ex)


P2.to_excel('./Producto2.xlsx')





'''
indicador 6

subset (tipo_seguimiento == 'caso')

extraigo el valor del CONTACTO (que esta en el folio)
cuantos casos tienen algún contacto asociado

78% app +- 3

'''
m = '7909170-7'




#vigente no eliminado
#confirmados no pb
#validado o inconcluso
#domicilio o residencia
#
#
#















