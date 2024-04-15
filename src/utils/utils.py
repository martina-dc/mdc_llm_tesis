import pandas as pd


def get_attributes_from_row(row):
    fila = dict()
    for e in row:
        for element in e:
            #print(element)
            fila[str(element['name'])] =  element['value_name']
    
    return pd.DataFrame.from_dict(fila, orient='index').transpose()


        
#Definimos una función para traer el elemento que queremos
def traerdato(elemento,rama,subrama,valor='value_name'):
    indices=[]
    for i,s in enumerate(elemento[rama]):
        for j in s:
            if subrama in str(s[j]):
                indices.append([i,s])
    if len(indices) == 0:
        return 'Sin Datos'
    else:
        return indices[0][1][valor]
    

 

def normalizar_lineas_procesador(df: pd.DataFrame, col_original: str)-> pd.DataFrame:
    """
    Esta funcion recibe un dataframe y devuelve el mismo DF con una columna de linea de procesador que esta normalizada.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe que debe tener una colummna llamada linea_procesador_

    Returns
    -------
    pd.DataFrame
        Dataframe con columna cambiada
    """
    condic_i3 = df[col_original].str.lower().str.contains("i3", na=False)
    condic_i5 = df[col_original].str.lower().str.contains("i5", na=False)
    condic_i7 = df[col_original].str.lower().str.contains("i7", na=False)
    condic_i9 = df[col_original].str.lower().str.contains("i9", na=False)

    df["linea_procesador_"] = df[col_original].copy()
    df.loc[condic_i3, "linea_procesador_"] = "Core i3"
    df.loc[condic_i5, "linea_procesador_"] = "Core i5"
    df.loc[condic_i7, "linea_procesador_"] = "Core i7"
    df.loc[condic_i9, "linea_procesador_"] = "Core i9"

    cond_ryzen = df[col_original].str.lower().str.contains("ryzen", na=False)
    condic_r3 = df[col_original].str.lower().str.contains("3", na=False)
    condic_r5 = df[col_original].str.lower().str.contains("5", na=False)
    condic_r7 = df[col_original].str.lower().str.contains("7", na=False)
    condic_r9 = df[col_original].str.lower().str.contains("9", na=False)


    df.loc[condic_r3 & cond_ryzen, "linea_procesador_"] = "Ryzen 3"
    df.loc[condic_r5& cond_ryzen, "linea_procesador_"] = "Ryzen 5"
    df.loc[condic_r7& cond_ryzen, "linea_procesador_"] = "Ryzen 7"
    df.loc[condic_r9& cond_ryzen, "linea_procesador_"] = "Ryzen 9"

    condic_a6 = df[col_original].str.lower().str.contains("a6", na=False)
    condic_a8 = df[col_original].str.lower().str.contains("a8", na=False)
    condic_a10 = df[col_original].str.lower().str.contains("a10", na=False)
    condic_a12 = df[col_original].str.lower().str.contains("a12", na=False)


    df.loc[condic_a6, "linea_procesador_"] = "AMD A6"
    df.loc[condic_a8, "linea_procesador_"] = "AMD A8"
    df.loc[condic_a10, "linea_procesador_"] = "AMD A10"
    df.loc[condic_a12, "linea_procesador_"] = "AMD A12"

    cond_celeron = df[col_original].str.lower().str.contains("celeron", na=False)
    df.loc[cond_celeron, "linea_procesador_"] = "Celeron"

    cond_pentium = df[col_original].str.lower().str.contains("pentium", na=False)
    df.loc[cond_pentium, "linea_procesador_"] = "Pentium"

    cond_athlon = df[col_original].str.lower().str.contains("athlon", na=False)
    df.loc[cond_athlon, "linea_procesador_"] = "Athlon"

    cond_sempron = df[col_original].str.lower().str.contains("sempron", na=False)
    df.loc[cond_sempron, "linea_procesador_"] = "Sempron"

    cond_m1 = df[col_original].str.lower().str.contains("m1", na=False)
    df.loc[cond_m1, "linea_procesador_"] = "M1"

    cond_m2 = df[col_original].str.lower().str.contains("m2", na=False)
    df.loc[cond_m2, "linea_procesador_"] = "M2"

    proc = df.linea_procesador.value_counts().to_frame()
    proc = df.linea_procesador_.value_counts().to_frame()
    proc["condicion"] = proc.index
    proc.loc[proc["linea_procesador_"] < 40, "condicion"] = "Otro"

    proc = proc.rename_axis('linea_procesador_Original').reset_index()
    proc.columns = ["linea_procesador_Original", "Cantidad", "linea_procesador_Nueva"]
    del proc["Cantidad"]

    df = df.merge(proc,
        how = "left",
        left_on = "linea_procesador_",
        right_on = "linea_procesador_Original")

    del df["linea_procesador_Original"],  df[col_original], df["linea_procesador_"]


    df = df.rename(columns = {"linea_procesador_Nueva"  : col_original}) 
    

    return df



def separar_valor_um(df: pd.DataFrame, 
                    colname: str, 
                    res_val: str, 
                    res_um: str,
                    cambio: dict,
                    new_um: str)->pd.DataFrame:
    """
    Esta funcion recibe una columna que tiene un valor y una unidad de medida y lo devuelve en dos columnas
    separadas. El nombre de la columna a tomar es colname y lo devuelve en res_val el numero y en res_um la unidad de medida. 
  

    Parameters
    ----------
    df : pd.DataFrame
        df con la columna a desglosar.
    colname : str
        nombre de la columna del df que tiene el valor combinado.
    res_val : str
        nuevo nombre que se le desea poner a la columna que tiene el valor.
    res_um : str
       nuevo nombre que se le desea poner a la columna que tiene la unidad de medida.

    cambio : dict
       Diccionario que tiene las equivalencias entre unidades de medida.
    
    new_um : str
       Nueva unidad de medida a la que llevaremos los valores.

    Returns
    -------
    pd.DataFrame
        _description_
    """

    lista = df[colname].str.split(' ', 1, expand=True)
    df[res_val], df[res_um] = lista[0], lista[1]

    for key, value in cambio.items():
        cond = (df[res_um].str.lower() == key)
        df.loc[cond, res_val] = df[res_val].fillna(-1).astype(float, errors = "ignore").astype(int) / value
        df.loc[cond, res_um] = new_um

 

    df[res_val] = df[res_val].fillna(-1).astype(float)

    return df


  