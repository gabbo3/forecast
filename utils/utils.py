import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd
import joblib
from datetime import timedelta

class forecast():
    """Forecast. Clase para proceso de etl y prediccion."""
    def __init__(self,path_diario,path_mensual,path_parcial,mes_anterior,model_path):
        self.path_diario = path_diario
        self.path_mensual = path_mensual
        self.model_path = model_path
        self.mes_anterior=mes_anterior
        self.session_diaria = path_diario
        self.session_mensual = path_mensual
        self.session_parcial = path_parcial
        
        

    def _etl_(self,x,y,z):
        return etl(
            session_diaria = x, 
            session_mensual = y,
            session_parcial = z
        )
    
    def _predict_(self, df):
        return predict(X = df,model_path = self.model_path,mes_anterior=self.mes_anterior)
    
    def _RunProcess_(self):
        try:
            x = self.session_diaria
            y = self.session_mensual
            z = self.session_parcial
            X = self._etl_(x,y,z)
            p_avg, p =  self._predict_(df = X)
        except Exception as e:
            print(e)
            raise e
            
        return p_avg, p

def dailyvars(x:pd.DataFrame):
    """Create variabvles from daily files, like diario y parcial mensual."""
    x.loc[:,"FechaFiltro"] = pd.to_datetime(x.FechaFiltro)
    x["aniomes"] = x.FechaFiltro.dt.year * 100 + x.FechaFiltro.dt.month
       
    for col in ['users','sessions','sessions_median','pageviews']:
        x.loc[:,f'{col}_diff_7d'] = x.groupby('aniomes')[col].diff(periods = 7)
        x.loc[:,f'{col}_diff_3d'] = x.groupby('aniomes')[col].diff(periods = 3)
        x.loc[:,f'{col}_diff_1d'] = x.groupby('aniomes')[col].diff(periods = 1)

        x.loc[:,f'{col}_median_window_7D'] = x.groupby('aniomes')[col].\
        rolling(window = 7).median().reset_index(0,drop=True)
        x.loc[:,f'{col}_cv_window_7D'] = x.groupby('aniomes')[col].rolling(window = 7,min_periods=1).std().reset_index(0,drop=True)\
        /x.groupby('aniomes')[col].rolling(window = 7,min_periods=1).mean().reset_index(0,drop=True)

        x.loc[:,f'{col}_median_window_3D'] = x.groupby('aniomes')[col].\
        rolling(window = 3).median().reset_index(0,drop=True)
        x.loc[:,f'{col}_cv_window_3D'] = x.groupby('aniomes')[col].rolling(window = 3,min_periods=1).std().reset_index(0,drop=True)\
        /x.groupby('aniomes')[col].rolling(window = 3,min_periods=1).mean().reset_index(0,drop=True)
    return x

def etl(session_diaria:pd.DataFrame,
        session_parcial:pd.DataFrame,
        session_mensual:pd.DataFrame):
    """Proceso de generacion de varables.
    session_diaria: pd.DataFrame con las mediciones diaria.
    session_mensual: pd.DataFrame con las mediciones mensuales.
    ------------------------------
    return.
    x: pd.DataFrame: variables para el modelo."""
    
    print(f'Filter monthly dataset by Origen != Otros')
    print(f'>>Dataset shape raw {session_mensual.shape}')
    session_mensual.loc[:,'Origen'] = session_mensual.Origen.apply(lambda x: normalize_origen(x))
    session_mensual = session_mensual.loc[session_mensual.Origen!="Otros"]
    session_mensual.loc[:,"FechaFiltro"] = pd.to_datetime(session_mensual.FechaFiltro)
    session_mensual["aniomes"] = session_mensual.FechaFiltro.dt.year * 100 + \
    session_mensual.FechaFiltro.dt.month
    print(f'>>Dataset shape filter {session_mensual.shape}')
    
    print(f'>>Dataset shape raw daily {session_diaria.shape}')
    session_diaria.loc[:,'Origen'] = session_diaria.Origen.apply(lambda x: normalize_origen(x))
    session_diaria = session_diaria.loc[session_diaria.Origen!="Otros"]
    session_diaria.loc[:,"FechaFiltro"] = pd.to_datetime(session_diaria.FechaFiltro)
    session_diaria["aniomes"] = session_diaria.FechaFiltro.dt.year * 100 + \
    session_diaria.FechaFiltro.dt.month
    print(f'>>Dataset shape {session_diaria.shape}')
    print(f'>>Dataset shape raw {session_parcial.shape}')
    session_parcial.loc[:,'Origen'] = session_parcial.Origen.apply(lambda x: normalize_origen(x))
    session_parcial = session_parcial.loc[session_parcial.Origen!="Otros"]
    session_parcial.loc[:,"FechaFiltro"] = pd.to_datetime(session_parcial.FechaFiltro)
    session_parcial["aniomes"] = session_parcial.FechaFiltro.dt.year * 100 + \
    session_parcial.FechaFiltro.dt.month
    print(f'>>Dataset shape filter {session_parcial.shape}')
    
    session_mensual_ = session_mensual.sort_values(by = 'FechaFiltro')
    session_mensual_ = session_mensual_.groupby('aniomes').agg(Users_D1 = ('Users','sum')).reset_index()
    
    print(f'step 1')
    for i in np.arange(1,4,1):
        session_mensual_.loc[:,f'aniomes_{i}'] =  pd.to_datetime(
            session_mensual_.aniomes, format = '%Y%m') + pd.DateOffset(months=i)
        session_mensual_.loc[:,f'aniomes_{i}'] = [int(x.strftime('%Y%m')) \
                                                  for x in session_mensual_[f'aniomes_{i}']
                                                 ]
    print(f'step 2')
    session_diaria_proc = session_diaria\
    .groupby("FechaFiltro").agg(
        aniomes = ('aniomes',lambda x: int(np.mean(x))),
        users = ('Users',np.nansum),
        sessions = ('Sessions',np.nansum),
        sessions_median = ('Sessions',np.nanmedian),  
        pageviews = ('Pageviews',np.nansum),
        origens = ('Origen','nunique') 
    ).reset_index()
    session_parcial_proc = session_parcial\
    .groupby("FechaFiltro").agg(
        aniomes = ('aniomes',lambda x: int(np.mean(x))),
        users = ('Users',np.nansum),
        sessions = ('Sessions',np.nansum),
        sessions_median = ('Sessions',np.nanmedian),  
        pageviews = ('Pageviews',np.nansum),
        origens = ('Origen','nunique') 
    ).reset_index()
    print(f'step 3')
    cols = ['Users_D1','aniomes_1','aniomes_2','aniomes_3']
    for i in np.arange(1,4,1):
        session_diaria_proc = session_diaria_proc.merge(session_mensual_[['Users_D1',f'aniomes_{i}']],
                                                        left_on= 'aniomes',
                                                        right_on= f'aniomes_{i}',
                                                        how = 'left')
        session_diaria_proc.rename(
            columns = {'Users_D1':f'Users_d{i}'},
            inplace = True)
        session_diaria_proc.drop(labels=f'aniomes_{i}', axis = 1 ,inplace=True)
        
    
    print(f'step 4')
    col = ['aniomes','FechaFiltro',
           'users','sessions',
           'sessions_median',
           'pageviews','origens','Users_d1',
           'Users_d2','Users_d3']
    
    x = session_diaria_proc[col]
    
    u = (pd.to_datetime(x.FechaFiltro,format="%Y%m") + MonthEnd(1)).dt.day
    v = x.FechaFiltro.dt.day
    x.loc[:,'frac_of_month'] = [np.round(vi/ui,2) for ui,vi in zip(u,v)]
    x.loc[:,'cum_sum_users'] = x.groupby('aniomes')['users'].cumsum()
    x.loc[:,'cum_sum_sessions'] = x.groupby('aniomes')['sessions'].cumsum()
    x.loc[:,'weekday'] = [u for u in x.FechaFiltro.dt.dayofweek]
    x.loc[:,'month'] = [u for u in x.FechaFiltro.dt.month]
    x.loc[:,'days_end_month'] = (x['FechaFiltro'] + pd.offsets.MonthEnd(0)\
                                 - x['FechaFiltro']).dt.days
    print(f'step 5')
    x = dailyvars(x)
    x.loc[:,'delta_d1_d2'] = x['Users_d1'] - x['Users_d2']
    x.loc[:,'delta_d2_d3'] = x['Users_d2'] - x['Users_d3']
    x.loc[:,'r_delta_d1_d2'] = x['Users_d1'].div(x['Users_d2']).replace(np.inf, np.nan)
    x.loc[:,'r_delta_d2_d3'] = x['Users_d2'].div(x['Users_d3']).replace(np.inf, np.nan)
    
    print(f'step 6')
    col = ['aniomes','FechaFiltro',
           'users','sessions',
           'sessions_median',
           'pageviews','origens']
    
    y = session_parcial_proc[col]
    y = dailyvars(y)
    for cols in y.columns:
        if cols not in ['FechaFiltro','aniomes']:
            y.rename( {cols: cols + '_partial' },
                     axis='columns',
                     inplace=True
                    )
    print(f'step 7')
    x = x.merge(y, on = ['FechaFiltro','aniomes'], how = 'left',validate='1:1')  
    cols = ['users','sessions','sessions_median','pageviews','origens']
    for col in cols:
        x.loc[:,f'{col}_daily_vs_partial'] = x[col]-x[col + '_partial']
        x.loc[:,f'{col}_daily_vs_partial_div'] = (x[col]-x[col + '_partial'])/(x[col + '_partial']+1)
    
    c = []
    for i in x.FechaFiltro:
        delta = i-pd.DateOffset(months=1)
        if len(x.loc[x.FechaFiltro== delta].users_partial.values) > 0:
            c.append(x.loc[x.FechaFiltro== delta].users_partial.values[0])
        else:
            c.append(np.nan)
    
    x.loc[:,'partial_today_vs_lastmonth'] = c
    x.loc[:,'partial_today_vs_lastmonth'] = x.partial_today_vs_lastmonth -x.users_partial
    x.loc[:,'partial_today_vs_lastmonth_ratio'] = x.users_partial/x.partial_today_vs_lastmonth 
    x.loc[:,'total_m0_vs_parcial'] = x.users_partial/x.Users_d1

    return x

def master(session_mensual:pd.DataFrame,x:pd.DataFrame):
    """Generacion de tabla maestra para entrenamiento.
    Join de variables mas target.
    session_mensual: pd.DataFrame: valores mensuales.
    x:pd.DataFrame:variables"""
    
    session_mensual.loc[:,'Origen'] = session_mensual.Origen.apply(lambda x: normalize_origen(x))
    session_mensual = session_mensual.loc[session_mensual.Origen!="Otros"]
    session_mensual.loc[:,"FechaFiltro"] = pd.to_datetime(session_mensual.FechaFiltro)
    session_mensual["aniomes"] = session_mensual.FechaFiltro.dt.year * 100 + \
    session_mensual.FechaFiltro.dt.month
    
    session_mensual = session_mensual.sort_values(by = 'FechaFiltro')   
    target = session_mensual.groupby('aniomes').agg(Y = ('Users','sum')).reset_index()
    target.loc[:,'Y_DIFF'] = target.Y.diff()
    return x.merge(target, on = 'aniomes', how = 'left')

def predict(X:pd.DataFrame,model_path:str,mes_anterior:int):
    """Permite realizar el predict utilizando un modelo pre entrenado
    X:pd.DataFrame: Dataframe con variables (etl)
    model_path: str: path al modelo.pkl
    """
    model = joblib.load(model_path)
    #TODO. SI se predice deltas poder sumar el valor anterior.
    X['FORECAST'] = mes_anterior + model.predict(X[model.feature_name_])
    #X.loc[:,'FORECAST'] = X['FORECAST']*.5
    p_avg = X.groupby('aniomes').FORECAST.apply(lambda x: int(np.mean(x[:7]))).reset_index()
    p = X.loc[X.aniomes == np.max(X.aniomes)][['FechaFiltro','FORECAST']]
    return p_avg, p
    
def normalize_origen(x):
    
    origens = ['RED CienRadios']
    
    if x not in origens:
        x = 'Otros'
    return x