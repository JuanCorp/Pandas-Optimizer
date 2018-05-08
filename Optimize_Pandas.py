import pandas as pd
import numpy as np
### Optimize Pandas Dataframe memory usage based on column value distribution.


def Determine_Type(series):
    if series.dtype in [np.int8,np.int16,np.int32,np.int64]:
        return "int"
    elif series.dtype in [np.float16,np.float32,np.float64]:
        return "float"
    else:
        return "object"
    
    
def Optimize_Int(series):
    distribution = series.describe()
    unsigned = distribution['min'] < 0
    final_type = None
    if len(series.unique()) == 2:
        final_type = "bool"
    elif unsigned and distribution['max'] < 256:
        final_type = "uint8"
    elif distribution['max'] < 128:
        final_type = "int8"
    elif unsigned and distribution['max'] < 65536:
        final_type = "uint16"
    elif distribution['max'] < 32768:
        final_type = "int16"
        
    elif unsigned and distribution['max'] < 4294967296:
        final_type = "uint32"
    elif distribution['max'] <  2147483648:
        final_type = "int32"
        
    elif unsigned:
        final_type = "uint64"
    else:
        final_type = "int64"
    
    return final_type

def Optimize_Float(series):
    distribution = series.describe()
    final_type = None
    if distribution['min'] > -100 and distribution['max'] < 100:
        final_type = 'float16'
    
    elif distirbution['min'] > -1e6 and distribution ['max'] < 1e6:
        final_type = 'float32'
    else:
        final_type = 'float64'
    return final_type

def Optimize_Object(series):
    
    #Dates
    try:
        pd.to_datetime(series)
        return "datetime"
    except:
        pass
    
    #Binary
    if len(series.unique()) == 2:
        return "bool"
    
    if len(series.unique()) >= len(series) /2:
        return "drop"
    else:
        return "category"
    
    

def Optimize_DF(dataframe):
    for col in dataframe.columns:
        col_type = Determine_Type(dataframe[col])
        
        if col_type == 'int':
            optimal_type = Optimize_Int(dataframe[col])
            dataframe[col] = dataframe[col].astype(optimal_type)

        elif col_type == 'float':
            optimal_type = Optimize_Float(dataframe[col])
            dataframe[col] = dataframe[col].astype(optimal_type)
        else:
            optimal_type = Optimize_Object(dataframe[col])
            if optimal_type == 'drop':
                dataframe.drop(col,axis=1,inplace=True)
            elif optimal_type == 'bool':
                values = dataframe[col].unique()
                dataframe[col].replace({values[0]:0,values[1]:1},inplace = True)
            elif optimal_type == 'datetime':
                dataframe[col] = pd.to_datetime(dataframe[col])
            else:
                dataframe[col] = dataframe[col].astype(optimal_type)
                
    return dataframe
        
    
