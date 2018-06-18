#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@time: 2018/05/{DAY}
"""


import pandas as pd
import numpy as np

def importance_features_map(df, regressor, y_label):
    X_columns = list(df.columns)
    X_columns.remove(y_label)
    importances_values = regressor.featureImportances
    importances_values = importances_values.toArray()

    print('importances_values_dtype',importances_values.dtype)

    arr = np.array([X_columns, importances_values])
    importance_map_df = pd.DataFrame(arr.T, columns=['X_columns', 'importances_values'])
    importance_map_df = importance_map_df.sort_values(by='importances_values', ascending=False)
    print('importance_map_df=======\n',importance_map_df)

    return importance_map_df