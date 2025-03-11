#!/usr/bin/env python
# coding: utf-8

# In[1]:


print("Staring FinFcst Forecast OAF process ...")


# In[2]:


import sys
import teradataml as tdml
from sqlalchemy import case, extract, cast, Date, func

import pandas as pd
import getpass
import json
import time
import math
import numpy as np
import time


# In[3]:


# Will switch to input Args when convert to python script
fcst_start_dt = '2024-04-01'  # Arg 1
fcst_month = 12               # Arg 2
valid_months = 3              # Arg 3
BudgetCase = "2024040"        # Arg 4


# # Connect to Vantage

# In[4]:


with open('vars_lake.json', 'r') as f:
    session_vars = json.load(f)

conn = tdml.create_context(host = session_vars['vantage']['host'], 
                           username = session_vars['vantage']['username'], 
                           password = session_vars['vantage']['password'],
                           database = session_vars['vantage']['database'],
                           logmech = session_vars['vantage']['Logmech'],  
                           tmode = session_vars['vantage']['TDmode']
                          )
print(conn)


# # Setting

# In[5]:


username = session_vars['vantage']['username']
database=session_vars['vantage']['database']
oaf_env_name = "FinFcst_env"
username = session_vars['vantage']['username']
output_database = session_vars['vantage']['output_database']

key_cols= [
'PnL_Group',
'PnL_Category',
'PnL_Line',
'PnL_Subline',
'Product_Period',
'Expirations',
'NumberOfPeriods',
'Company',
'Customer_Type',
'finregion_2016']
#'IsPrimaryProduct',
#'isFreeRevenue',
#'free_trial_flag'
time_col = 'Renewal_Month'

input_tbl = "vw_renewal_model_aggregate_R360"

# all staging tables
flt_tbl = "finfcst_filter_inp_vt"
ym_tbl = "finfcst_yrmnt_vt"
map_key_stage_tbl = "FinFcst_key_stg"
stage_tbl = "finfcst_stage_tbl"
arima_tbl = "finfcst_arima_fcst_tbl"
prophet_tbl = "finfcst_prophet_fcst_tbl"
theta_tbl = "finfcst_theta_fcst_tbl"

modelscore_tbl = "finfcst_model_score_tbl"
champion_tbl = "rptFinBudget_MLRenewalOutput"


# Check and Start Compute Cluster if necessary

# In[6]:


def is_compute_cluster_running(cp_name):
    SQL1 = f"""SELECT ComputeProfileState
    FROM DBC.ComputeProfilesV
    WHERE ComputeProfileName='{cp_name}';
    """
    SQL2 = f"""SELECT CurrentState
    FROM DBC.computestatusV
    WHERE ComputeProfileName='{cp_name}';
    """
    cp_df = tdml.DataFrame.from_query(SQL1).to_pandas()
    cp_status = cp_df['ComputeProfileState'].iloc[0]
    cc_df = tdml.DataFrame.from_query(SQL2).to_pandas()
    cc_status = cc_df['CurrentState'].iloc[0]

#    if (cp_status=='Running' & cc_status=='ACTIVE'):
    if (cp_status=='Running') & (cc_status=='ACTIVE'):
        return(True)
    else:
        return(False)


# In[7]:


if (not(is_compute_cluster_running(session_vars['oaf']['cp']))):
    print("Not running")
else:
    print("running")


# In[8]:


#resume_SQL = f"""RESUME COMPUTE FOR COMPUTE PROFILE "{session_vars['oaf']['cp']}" IN COMPUTE GROUP "{session_vars['oaf']['cc']}" """
#sleep_time = 30

#if (not(is_compute_cluster_running(session_vars['oaf']['cp']))):
#    print(f"Resuming Compute Profile [{session_vars['oaf']['cp']}] now...")
#    tdml.execute_sql(resume_SQL)
#    time.sleep(sleep_time)
#    while (not(is_compute_cluster_running(session_vars['oaf']['cp']))):
#        time.sleep(sleep_time)
#else:
#    print(f"Compute Profile [{session_vars['oaf']['cp']}] is running now.")


# # Prepare the input

# ### Prepare the data from input

# In[9]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {username}.{flt_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET VOLATILE TABLE {username}.{flt_tbl} AS (
SELECT
  {",".join(key_cols+[time_col])},
  tot_expiration_units, 
  (CASE WHEN {time_col} < CAST('{fcst_start_dt}' AS DATE FORMAT 'YYYY-MM-DD') THEN tot_renewal_units ELSE NULL END) AS tot_renewal_units ,
  SUM(CASE WHEN {time_col} < CAST('{fcst_start_dt}' AS DATE FORMAT 'YYYY-MM-DD') THEN 1 ELSE NULL END) 
     OVER (PARTITION BY {",".join(key_cols)}) AS hist_cnt,
  MIN({time_col}) OVER () AS st_date,
  (CASE WHEN {time_col} < CAST('{fcst_start_dt}' AS DATE FORMAT 'YYYY-MM-DD') THEN 1 ELSE 0 END) AS historial_period
FROM
(SELECT
  {",".join(key_cols+[time_col])},
  SUM(expiration_units) AS tot_expiration_units,
  SUM(renewal_units) AS tot_renewal_units
FROM {session_vars['vantage']['src_database']}.{input_tbl}
WHERE {time_col} < ADD_MONTHS(CAST('{fcst_start_dt}' AS DATE FORMAT 'YYYY-MM-DD'), {fcst_month})
AND Customer_Type <> 'Not Evaluated'
AND IsPrimaryProduct = 1
AND isFreeRevenue = 0
AND free_trial_flag = 0
GROUP BY {",".join(key_cols+[time_col])}
) t
) WITH DATA
NO PRIMARY INDEX
ON COMMIT PRESERVE ROWS;
"""
print(SQL)
tdml.execute_sql(SQL)
### display the size of output table
print(f"Created table {flt_tbl} with size={tdml.DataFrame(tdml.in_schema(username,flt_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# ### Mapping Year/Month to t

# In[10]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {username}.{ym_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET VOLATILE TABLE {username}.{ym_tbl} AS (
SELECT
  calendar_date AS {time_col}, 
  t,
  MAX(CASE WHEN calendar_date<=end_date THEN t ELSE NULL END) OVER () AS act_max_t
FROM
(SELECT c.calendar_date, 
RANK() OVER (ORDER BY c.calendar_date) AS t,
m.st_date, m.end_date
FROM Sys_Calendar.Calendar c, 
(SELECT MIN(renewal_month) as st_date, MAX(renewal_month) AS end_date
FROM {username}.{flt_tbl}
WHERE historial_period=1) m
WHERE c.day_of_month=1
AND c.calendar_date >= m.st_date) t
QUALIFY t<=(act_max_t+{fcst_month})
) WITH DATA
UNIQUE PRIMARY INDEX (t)
ON COMMIT PRESERVE ROWS
"""

print(SQL)
tdml.execute_sql(SQL)
### display the size of output table
print(f"Created table {ym_tbl} with size={tdml.DataFrame(tdml.in_schema(username,ym_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[11]:


yrmth_df = tdml.DataFrame(tdml.in_schema(username,ym_tbl)).select("act_max_t").to_pandas()
fcst_yrmnth = yrmth_df["act_max_t"].iloc[0] + 1
print(f"1st Forecast month = {fcst_yrmnth}")


# ### Generete a Unique Key for each forecasting 

# In[12]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {username}.{map_key_stage_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET VOLATILE TABLE {username}.{map_key_stage_tbl} AS (
SELECT
  {",".join(key_cols)}, hist_cnt,
  RANK() OVER (ORDER BY  {",".join(key_cols)} ) AS uID
FROM
(SELECT DISTINCT
  {",".join(key_cols)}, hist_cnt
FROM {username}.{flt_tbl}
--WHERE hist_cnt>=10 
) t
) WITH DATA
UNIQUE PRIMARY INDEX (uID)
ON COMMIT PRESERVE ROWS
"""

print(SQL)
tdml.execute_sql(SQL)
### display the size of output table
print(f"Created table {map_key_stage_tbl} with size={tdml.DataFrame(tdml.in_schema(username,map_key_stage_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# ### Generete input for forecasting model

# In[13]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{stage_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET TABLE {output_database}.{stage_tbl} AS (
SELECT
  f.{", f.".join(key_cols)}, 
  f.{time_col}, 
  k.uID,
  c.t,
  tot_expiration_units,
  tot_renewal_units,
  (CASE WHEN tot_expiration_units>0 THEN CAST(tot_renewal_units AS FLOAT)/tot_expiration_units ELSE 0.00 END) AS renewal_rate,
  k.hist_cnt
FROM {username}.{flt_tbl} f, {username}.{map_key_stage_tbl} k, {username}.{ym_tbl} c
WHERE f.{time_col} = c.{time_col}
AND {" AND ".join(["f."+x+"=k."+x for x in key_cols])}
AND f.historial_period = 1
) WITH DATA
PRIMARY INDEX (uID);
"""

print(SQL)
tdml.execute_sql(SQL)
### display the size of output table
print(f"Created table {stage_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,stage_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[14]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{stage_tbl}_v2")
except:
    pass
    
SQL = f"""CREATE MULTISET TABLE {output_database}.{stage_tbl}_v2 AS (
SELECT 
  u.uID,
  u.t, 
  CAST(CAST(u.Renewal_month AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)) As ds, 
  COALESCE(renewal_rate, 0.00) AS renewal_rate,
  u.hist_cnt,
  COUNT(0) OVER (PARTITION BY u.uID) AS hist_cnt_v2
FROM
  (SELECT k.uID, c.t, c.Renewal_month, k.hist_cnt
   FROM
  (SELECT uID, min(Renewal_month)AS st_Renewal_month, max(hist_cnt) AS hist_cnt
   FROM {output_database}.{stage_tbl}
   GROUP BY uID) k,
  {username}.{ym_tbl} c
WHERE c.Renewal_month >= k.st_Renewal_month
AND   c.t<=act_max_t) u
LEFT JOIN {stage_tbl} i
ON (u.uID = i.uID
AND u.Renewal_month = i.Renewal_month)
) WITH DATA
PRIMARY INDEX (uID)
"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{stage_tbl}_v2 INDEX (uID)")
### display the size of output table
print("--- %s seconds ---" % (time.time() - start_time))


# ### Callling OAF container for renewal forecast

# In[15]:


# Select the Analytics Compute Cluster
tdml.execute_sql(f'SET SESSION COMPUTE GROUP "{session_vars['oaf']['cc']}";')
tdml.DataFrame.from_query(f'''SELECT UserName, CurrentComputeGroup
FROM dbc.sessioninfo
where username='{session_vars['vantage']['username']}'
AND clientVmName like '%python%'
''')


# In[16]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{arima_tbl}_oaf")
except:
    pass
print("Calling AUTO-ARIMA forecast within Vantage OAF...")
SQL = f"""CREATE MULTISET TABLE {output_database}.{arima_tbl}_oaf AS (
SELECT 
  f.uID, f.t, f.retval
FROM Apply(
	ON (SELECT uID, t, CAST(renewal_rate AS DECIMAL(7,3)) AS Renewal_Rate 
        FROM {output_database}.{stage_tbl}_v2
        WHERE hist_cnt_v2 >= 10) AS "input"
	HASH BY uID
    LOCAL ORDER BY uID, t
	returns(uID INTEGER, t VARCHAR(20), retval FLOAT)
	USING
	APPLY_COMMAND('python3 calc_arima.py {fcst_yrmnth} {fcst_month} {valid_months}')
	ENVIRONMENT('{oaf_env_name}')
	STYLE('csv')
	delimiter(',') 
    ) as f
) WITH DATA
PRIMARY INDEX (uID)
"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{arima_tbl}_oaf INDEX (uID)")
### display the size of output table
print("--- %s seconds ---" % (time.time() - start_time))


# In[17]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{arima_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET TABLE {output_database}.{arima_tbl} AS (
SELECT
  t.{", t.".join(key_cols)},
  t.{time_col},
  t.uID, t.t, (CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) AS Renewal_rate,
  i.tot_expiration_units AS expiries,
  ((CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) * i.tot_expiration_units) AS Renewal
FROM 
(SELECT 
    k.{", k.".join(key_cols)},
    c.{time_col},
    f.uID, f.t,
    f.retval AS Renewal_rate
FROM {output_database}.{arima_tbl}_oaf f, 
     {username}.{map_key_stage_tbl} k, 
     {username}.{ym_tbl} c
WHERE f.uID = k.uID
AND CAST(f.t AS INTEGER) = c.t
AND f.t NOT IN ('RMSE') )t,
{flt_tbl} i
WHERE {" AND ".join(['t.'+x+"=i."+x for x in key_cols+[time_col]])}
) WITH DATA
PRIMARY INDEX (uID)
"""

print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{arima_tbl} INDEX (uID)")
### display the size of output table
print(f"Created table {arima_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,arima_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[18]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{prophet_tbl}_oaf")
except:
    pass
print("Calling Prophet forecast within Vantage OAF...")
SQL = f"""CREATE MULTISET TABLE {output_database}.{prophet_tbl}_oaf AS (
SELECT
  f.uID, f.t, f.retval
FROM Apply(
	ON (SELECT uID, ds, renewal_rate 
        FROM {output_database}.{stage_tbl}_v2
        WHERE hist_cnt_v2 >= 10) AS "input"
	HASH BY uID
    LOCAL ORDER BY uID, ds
	returns(uID INTEGER, t VARCHAR(100), retval FLOAT)
	USING
	APPLY_COMMAND('python3 calc_prophet.py {fcst_yrmnth} {fcst_month} {valid_months}')
	ENVIRONMENT('{oaf_env_name}')
	STYLE('csv')
	delimiter(',') 
    ) as f
) WITH DATA
PRIMARY INDEX (uID)"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{prophet_tbl}_oaf INDEX (uID)")
print("--- %s seconds ---" % (time.time() - start_time))


# In[19]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{prophet_tbl}")
except:
    pass

print("Creating the final Prophet forecast ...")
SQL = f"""CREATE MULTISET TABLE {output_database}.{prophet_tbl} AS (
SELECT
  t.{", t.".join(key_cols)},
  t.{time_col},
  t.uID, (CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) AS Renewal_Rate,
  i.tot_expiration_units AS expiries,
  ((CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) * i.tot_expiration_units) AS Renewal
FROM 
(SELECT 
    k.{", k.".join(key_cols)},
    CAST(SUBSTR(f.t,1,10) AS DATE) AS {time_col},
    f.uID, 
    f.retval AS Renewal_rate
FROM {output_database}.{prophet_tbl}_oaf  f, 
     {username}.{map_key_stage_tbl} k
WHERE f.uID = k.uID
AND   f.t NOT IN ('RMSE') )t,
{flt_tbl} i
WHERE {" AND ".join(['t.'+x+"=i."+x for x in key_cols+[time_col]])}
) WITH DATA
PRIMARY INDEX (uID)
"""

print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{prophet_tbl} INDEX (uID)")
### display the size of output table
print(f"Created table {prophet_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,prophet_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[20]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{theta_tbl}_oaf")
except:
    pass
    
print("Calling Optimized Theta forecast within Vantage OAF...")
SQL = f"""CREATE MULTISET TABLE {output_database}.{theta_tbl}_oaf AS (
SELECT
  f.uID, f.t, f.retval
FROM Apply(
	ON (SELECT uID, ds, renewal_rate 
        FROM {output_database}.{stage_tbl}_v2 
        WHERE hist_cnt_v2 >=10 ) AS "input"
	HASH BY uID
    LOCAL ORDER BY uID, ds
	returns(uID INTEGER, t VARCHAR(100), retval FLOAT)
	USING
	APPLY_COMMAND('python3 calc_theta.py {fcst_yrmnth} {fcst_month} {valid_months}')
	ENVIRONMENT('{oaf_env_name}')
	STYLE('csv')
	delimiter(',') 
    ) as f
) WITH DATA
PRIMARY INDEX (uID)"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{theta_tbl}_oaf INDEX (uID)")

print("--- %s seconds ---" % (time.time() - start_time))


# In[21]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{theta_tbl}")
except:
    pass
    
print("Creating the final Theta forecast ...")
SQL = f"""CREATE MULTISET TABLE {output_database}.{theta_tbl} AS (
SELECT
  t.{", t.".join(key_cols)},
  t.{time_col},
  t.uID, (CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) AS Renewal_rate,
  i.tot_expiration_units AS expiries,
  ((CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) * i.tot_expiration_units) AS Renewal
FROM 
(SELECT 
    k.{", k.".join(key_cols)},
    CAST(SUBSTR(f.t,1,10) AS DATE) AS {time_col},
    f.uID, 
    f.retval AS Renewal_rate
FROM {output_database}.{theta_tbl}_oaf  f, 
     {username}.{map_key_stage_tbl} k
WHERE f.uID = k.uID
AND   f.t NOT IN ('RMSE') )t,
{flt_tbl} i
WHERE {" AND ".join(['t.'+x+"=i."+x for x in key_cols+[time_col]])}
) WITH DATA
PRIMARY INDEX (uID)
"""

print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON {output_database}.{theta_tbl} INDEX (uID)")### display the size of output table
print(f"Created table {theta_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,theta_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# ### Display Winning model

# In[22]:


start_time = time.time()

try:
    tdml.execute_sql(f"DROP TABLE {output_database}.{modelscore_tbl}")
except:
    pass

SQL = f"""CREATE MULTISET TABLE {output_database}.{modelscore_tbl} AS (
SELECT
  uID, ModelName, retval AS RMSE,
  RANK() OVER (PARTITION BY uID ORDER BY RMSE, ModelName) AS model_rnk
FROM 
(
SELECT
  uID, 'ARIMA'(VARCHAR(100)) AS ModelName, retval
FROM {output_database}.{arima_tbl}_oaf
WHERE t = 'RMSE'
UNION ALL
SELECT
  uID, 'Prophet'(VARCHAR(100)) AS ModelName, retval
FROM {output_database}.{prophet_tbl}_oaf
WHERE t = 'RMSE'
UNION ALL
SELECT
  uID, 'Theta'(VARCHAR(100)) AS ModelName, retval
FROM {output_database}.{theta_tbl}_oaf
WHERE t = 'RMSE'
) t
) WITH DATA
PRIMARY INDEX (uID)
"""

print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON  {output_database}.{modelscore_tbl} INDEX (uID)")

### display the size of output table
print(f"Created table {modelscore_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,modelscore_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[23]:


tdml.DataFrame.from_query(f"""SELECT 
  modelname, COUNT(0) AS rec_cnt
FROM  {output_database}.{modelscore_tbl}
WHERE model_rnk=1
GROUP BY 1""").sort("rec_cnt", ascending = False)


# In[24]:


start_time = time.time()

try:
    tdml.execute_sql(f"DELETE FROM {output_database}.{champion_tbl} WHERE BudgetCase = {BudgetCase}")
    tmpSQL_head = f"INSERT INTO {output_database}.{champion_tbl} "
    tmpSQL_tail = "" 
except:
    tmpSQL_head = f"CREATE MULTISET TABLE {output_database}.{champion_tbl} AS ( "
    tmpSQL_tail = " ) WITH DATA PRIMARY INDEX (uID)"

SQL = f"""{tmpSQL_head}
SELECT
  {",".join(key_cols+[time_col])},
  uID, 
  1 AS isPrimaryProduct,
  0 AS isFreeRevenue,
  Renewal_rate, expiries, Renewal,
  CAST({BudgetCase} AS INTEGER) AS BudgetCase,
  CURRENT_TIMESTAMP AS RunDate
FROM {output_database}.{arima_tbl}
WHERE uID IN (SELECT uID FROM {output_database}.{modelscore_tbl} WHERE modelname = 'ARIMA' AND model_rnk=1)
UNION ALL
SELECT
  {",".join(key_cols+[time_col])},
  uID, 
  1 AS isPrimaryProduct,
  0 AS isFreeRevenue,
  Renewal_rate, expiries, Renewal,
  CAST({BudgetCase} AS INTEGER) AS BudgetCase,
  CURRENT_TIMESTAMP AS RunDate
FROM {output_database}.{prophet_tbl}
WHERE uID IN (SELECT uID FROM {output_database}.{modelscore_tbl} WHERE modelname = 'Prophet' AND model_rnk=1)
UNION ALL
SELECT
  {",".join(key_cols+[time_col])},
  uID, 
  1 AS isPrimaryProduct,
  0 AS isFreeRevenue,
  Renewal_rate, expiries, Renewal,
  CAST({BudgetCase} AS INTEGER) AS BudgetCase,
  CURRENT_TIMESTAMP AS RunDate
FROM {output_database}.{theta_tbl}
WHERE uID IN (SELECT uID FROM {output_database}.{modelscore_tbl} WHERE modelname = 'Theta' AND model_rnk=1)
{tmpSQL_tail}"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON  {output_database}.{champion_tbl} INDEX (uID)")

### display the size of output table
print(f"Created table {champion_tbl} with size={tdml.DataFrame(tdml.in_schema(output_database,champion_tbl)).shape}")
print("--- %s seconds ---" % (time.time() - start_time))


# In[25]:


start_time = time.time()

print("Adding additional forecast for the missing ID")
SQL = f"""INSERT INTO {output_database}.{champion_tbl}
SELECT
  t.{",t.".join(key_cols+[time_col])},
  t.uID, 
  1 AS isPrimaryProduct,
  0 AS isFreeRevenue,
  (CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END) AS Renewal_rate, 
  i.tot_expiration_units AS expiries, 
  (i.tot_expiration_units * (CASE WHEN t.Renewal_rate BETWEEN 0.000 AND 1.000 THEN t.Renewal_rate ELSE 0.000 END)) AS Renewal,
  CAST({BudgetCase} AS INTEGER) AS BudgetCase,
  CURRENT_TIMESTAMP AS RunDate
FROM 
(SELECT
  k.{", k.".join(key_cols)},
  f.uID,  y.t, y.{time_col}, f.renewal_rate
FROM
  (SELECT uID, AVERAGE(renewal_rate) AS renewal_rate 
   FROM {output_database}.{stage_tbl}_v2
   WHERE uID NOT IN (SELECT uID FROM {output_database}.{champion_tbl} WHERE BudgetCase={BudgetCase} )
   AND t BETWEEN ({fcst_yrmnth} - 6) AND ({fcst_yrmnth}-1)
   GROUP BY uID) f, 
  {username}.{ym_tbl} y, 
  {username}.{map_key_stage_tbl} k
WHERE f.uID = K.uID 
AND y.t BETWEEN {fcst_yrmnth} AND ({fcst_yrmnth} + 12 - 1)
) t, {flt_tbl} i
WHERE {" AND ".join(['t.'+x+"=i."+x for x in key_cols+[time_col]])}
"""
print(SQL)
tdml.execute_sql(SQL)
tdml.execute_sql(f"COLLECT STAT ON  {output_database}.{champion_tbl} INDEX (uID)")

### display the size of output table
print("--- %s seconds ---" % (time.time() - start_time))


# Suspend the Compute Cluster

# In[26]:


# Select the Analytics Compute Cluster
tdml.execute_sql('SET SESSION COMPUTE GROUP NULL;')
tdml.DataFrame.from_query(f'''SELECT UserName, CurrentComputeGroup
FROM dbc.sessioninfo
where username='{session_vars['vantage']['username']}'
AND clientVmName like '%python%'
''')


# In[27]:


#suspend_SQL = f"""SUSPEND COMPUTE FOR COMPUTE PROFILE "{session_vars['oaf']['cp']}" IN COMPUTE GROUP "{session_vars['oaf']['cc']}" """
#print(suspend_SQL)
#tdml.execute_sql(suspend_SQL)


# # Disconnect from Vantage

# In[28]:


tdml.remove_context()


# In[29]:


print("FinFcst Forecast OAF process ran successfully.")


# In[ ]:




