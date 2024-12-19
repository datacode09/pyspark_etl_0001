from pyspark.sql import functions as F

# Load input datasets
extensions_df = dataiku.Dataset("cb_arm_models_dev_covid_relief_tf_extensions_pg").get_dataframe()
rpt_detail_df = dataiku.Dataset("cb_arm_models_dev_v_cmrl_esr_rpt_dtl_pq_partitioned").get_dataframe()

# Step 1: Calculate the minimum RUN_DATE for each APFI_ID
min_df = (
    extensions_df
    .filter(F.col("APFI_ID") != "NULL")
    .groupBy("APFI_ID")
    .agg(F.min("RUN_DATE").alias("RELF_STRT_DT"))
)

# Step 2: Select distinct columns from the second table
fcy_df = (
    rpt_detail_df
    .filter(F.col("FCY_ID").isNotNull())
    .select(
        "RPT_PRD_END_DT",
        "ESR_LNDG_TXN_FLG",
        "US_LNDG_TXN_FLG",
        "BRWR_ID_TP_CD",
        "BRWR_ID",
        "FCY_ID",
        "FCY_SK_ID",
        "SRC_STM_CD"
    ).distinct()
)

# Step 3: Join the two dataframes
result_df = (
    min_df.alias("a")
    .join(fcy_df.alias("c"), F.col("a.APFI_ID") == F.col("c.FCY_ID"), "left")
    .select(
        F.col("a.RUN_DATE"),
        F.col("a.APFI_ID").alias("FCY_SK_ID"),
        F.lit("APMS_RELIEF_TYPE").alias("RELF_TP_DSC_apms"),
        F.lit("TF EXCEL").alias("source"),
        F.lit("FACILITY").alias("level"),
        F.lit("PAYMENT").alias("RELF_TP_CD"),
        F.lit("COVID19 Principal and Interest Payment Deferral").alias("RELF_TP_DSC"),
        F.lit(90).alias("SM_APMS_DURATION_DAYS_CNT"),
        F.col("c.RPT_PRD_END_DT"),
        F.col("c.ESR_LNDG_TXN_FLG"),
        F.col("c.US_LNDG_TXN_FLG"),
        F.col("c.BRWR_ID_TP_CD"),
        F.col("c.BRWR_ID"),
        F.col("c.FCY_ID"),
        F.col("c.SRC_STM_CD")
    )
)

# Write the result to the output dataset
output_dataset = dataiku.Dataset("output_dataset_name")
output_dataset.write_with_schema(result_df)
