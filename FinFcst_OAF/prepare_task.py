def run_task(context: ModelContext, **kwargs):
    aoa_create_context()
        
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

    
    # your feature engineering code
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
    tdml.execute_sql(SQL)