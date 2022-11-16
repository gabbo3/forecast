from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import os
import traceback
from utils.utils import *
from SQLServer import SQLServer
# Logging Config
d = datetime.now()
logpath = os.getcwd() + "/logs/" + d.strftime('%Y%m%d')
os.makedirs(logpath, exist_ok=True)
logfilename = logpath + "/" + d.strftime('%Y%m%d%H%M%S') + "_" + 'predictivo' + ".log"
logname = d.strftime('%Y%m%d%H%M%S') + "_" + 'predictivo' + ".log"
print('Logging in: ' + logfilename)
logging.basicConfig(level=logging.INFO, filename=logfilename, format='%(levelname)s:%(asctime)s:%(funcName)s:%(lineno)d:%(message)s',datefmt='%Y-%m-%d %H:%M:%S')


#Execute

try:
    sql = SQLServer()

    n = 0

    since = format(datetime.today().replace(day=1), '%Y-%m-%d')
    three_months_ago = format(datetime.today() - relativedelta(months=3), '%Y-%m-%d')
    until = format(datetime.today() - timedelta(days=1+n), '%Y-%m-%d')

    df_diario = sql.query(f"select * from GA_DIARIO gm where origen = 'red cienradios' and fechafiltro between '{since}' and '{until}' order by FechaFiltro desc")
    df_mensual = sql.query(f"select * from GA_MENSUAL gm where origen = 'red cienradios' and fechafiltro between '2021-01-01' and '{until}' order by FechaFiltro desc")
    df_parcial = sql.query(f"select * from GA_MENSUALPARCIAL gm where origen = 'red cienradios' and fechafiltro between '{three_months_ago}' and '{until}' order by FechaFiltro desc")
    nro_mes_anterior = int(df_mensual.Users.iloc[0])

    df_diario = df_diario.astype({'Users':'int64'})
    df_diario = df_diario.astype({'Sessions':'int64'})
    df_diario = df_diario.astype({'Pageviews':'int64'})
    df_parcial = df_parcial.astype({'Users':'int64'})
    df_parcial = df_parcial.astype({'Sessions':'int64'})
    df_parcial = df_parcial.astype({'Pageviews':'int64'})
    df_mensual = df_mensual.astype({'Users':'int64'})
    df_mensual = df_mensual.astype({'Sessions':'int64'})
    df_mensual = df_mensual.astype({'Pageviews':'int64'})

    aForecast = forecast(df_diario, df_mensual, df_parcial, nro_mes_anterior, 'model_nov2022_08.pkl')

    p_avg, p = aForecast._RunProcess_()

    p_avg.rename(columns = {'aniomes':'UKEY', 'FORECAST':'Forecast'}, inplace = True)
    p.rename(columns = {'FechaFiltro':'UKEY', 'FORECAST':'Forecast'}, inplace = True)
    p.UKEY = p.UKEY.dt.strftime('%Y%m%d')

    p_avg['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    p['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')

    sql.upsert(p_avg, 'ga_forecast_avg')
    sql.upsert(p, 'ga_forecast')

except Exception as e:
	logging.error(e, exc_info=True)
	traceback.print_exc()

logging.info('FIN')
exit()



