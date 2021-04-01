import pandas as pd
import numpy as np

### """Setting options and vieweing data compiled through DEBIT.py and CREDIT.py""" ###

pd.set_option('display.max_columns', None)
dates_list = ['ACCOUNT_OPEN_DATE', 'CARD_ISSUE_DATE', 'CARD_EXPIRY_DATE', 'CARD_ACTIVATION_DATE', 'FIRST_TRN_DATE']

debit = pd.read_csv("DC_DATA.csv", sep=";", parse_dates=dates_list, low_memory=False, error_bad_lines=False)
debit = pd.DataFrame(debit)
credit = pd.read_csv("CC_DATA.csv", sep=";", parse_dates=dates_list, low_memory=False, error_bad_lines=False)
credit = pd.DataFrame(credit)
cards = pd.concat([debit,credit])
DATA = cards

def clean_amt(x):
if isinstance(x, str):
return(x.replace(",", "."))
return(x)

### """CLEANING DATA""" ###
DATA["ZIP"] = DATA["ZIP"].fillna(0).astype(int)
DATA["STREET_NUM"] = DATA["STREET_NUM"].fillna(0).astype(str)
DATA["STREET_NAME"] = DATA["STREET_NAME"].fillna(0).astype(str)
DATA["CITY_NAME"] = DATA["CITY_NAME"].fillna(0).astype(str)
DATA["PRODUCT"] = DATA["PRODUCT"].fillna(0).astype(str)
DATA["PRODUCT_TP"] = DATA["PRODUCT_TP"].fillna(0).astype(str)
DATA["CA_BAL"] = DATA["CA_BAL"].fillna(0).apply(clean_amt).astype(float).astype(int)
DATA["AVG_DR_BAL"] = DATA["AVG_DR_BAL"].fillna(0).apply(clean_amt).astype(float).astype(int)
DATA["CURRENT_BALANCE"] = DATA["CURRENT_BALANCE"].fillna(0).apply(clean_amt).astype(float).astype(int)
DATA["SRC_ID"] = DATA["SRC_ID"].fillna(0).astype(int).astype(str)
DATA["CURR_ACC_ID"] = DATA["CURR_ACC_ID"].fillna(0).astype(int).astype(str)
DATA["CARD_ACC_ID"] = DATA["CARD_ACC_ID"].fillna(0).astype(int).astype(str)
DATA["CONTR_NUM"] = DATA["CONTR_NUM"].fillna(0).astype(int).astype(str)


### """ADDING EXCLUDE TAG: TRN = 0 AND CURRENT BALANCE <10000""" ###
dc_exclude = DATA[(DATA["TOTAL_TRN_COUNT"]==0) & (DATA["CARD_CLASS_CODE"]=="DC") & (DATA["CA_BAL"]<10000)] 
dc_exclude = dc_exclude.groupby(["CARD_NUM"],sort=False).count().reset_index()
exclude = dc_exclude["CARD_NUM"]
exclude = pd.DataFrame(exclude)
exclude["EXCLUDE_INACTIVE"] = 1
DATA_exclude = DATA.merge(exclude, how="left", on="CARD_NUM")
DATA_exclude["EXCLUDE_INACTIVE"] = DATA_exclude["EXCLUDE_INACTIVE"].fillna(0).astype(int).astype(str)


### """ADDING PERCENTILE TAGS FOR TRANSACTIONS AND VOLUME""" ###
DATA_exclude["TRN_PERCENTILE_RANK"] = DATA_exclude.TOTAL_TRN_COUNT.rank(pct = True).round(1)
DATA_exclude["TRN_AMT_PERCENTILE_RANK"] = DATA_exclude.TOTAL_TRN_AMT.rank(pct= True).round(1)


### """ADDING CARD COUNT, CARD COUNT BY CLASS, CLASS COUNT""" ###
card_count = DATA_exclude.groupby(["UNI_PT_KEY"], sort=False)["CARD_NUM"].nunique().reset_index()
card_count.rename(columns = {"CARD_NUM": "CARD_COUNT"},inplace = True)
DATA_card_count = DATA_exclude.merge(card_count, on="UNI_PT_KEY", how="left")
card_class_count = DATA_card_count.groupby(["UNI_PT_KEY"], sort=False)["CARD_CLASS_CODE"].nunique().reset_index()
card_class_count.rename(columns = {"CARD_CLASS_CODE": "CARD_CLASS_COUNT"},inplace = True)
DATA_card_class_count = DATA_card_count.merge(card_class_count, on ="UNI_PT_KEY", how="left")
DATA_card_class_count["CARD_CLASS_COUNT"] = DATA_card_class_count["CARD_CLASS_COUNT"].astype(str)

card_count_by_class = DATA_card_count.groupby(["UNI_PT_KEY","CARD_CLASS_CODE"], sort=False)["CARD_NUM"].nunique().reset_index()
card_count_by_class.rename(columns = {"CARD_NUM": "CARD_COUNT_BY_CLASS"},inplace = True)
card_count_DC = card_count_by_class.loc[(card_count_by_class['CARD_CLASS_CODE'] == "DC" )]
card_count_DC = card_count_DC.groupby(["UNI_PT_KEY"])["CARD_COUNT_BY_CLASS"].sum().reset_index()
card_count_DC.rename(columns = {"CARD_COUNT_BY_CLASS": "CARD_COUNT_BY_CLASS_DC"},inplace = True)
card_count_DC = pd.DataFrame(card_count_DC)
DATA_card_class_count_dc = DATA_card_class_count.merge(card_count_DC, how="left", on="UNI_PT_KEY")

card_count_CC = card_count_by_class.loc[(card_count_by_class['CARD_CLASS_CODE'] == "CC" )]
card_count_CC = card_count_CC.groupby(["UNI_PT_KEY"])["CARD_COUNT_BY_CLASS"].sum().reset_index()
card_count_CC.rename(columns = {"CARD_COUNT_BY_CLASS": "CARD_COUNT_BY_CLASS_CC"},inplace = True)
card_count_CC = pd.DataFrame(card_count_CC)
DATA_card_class_count_cc = DATA_card_class_count_dc.merge(card_count_CC, how="left", on="UNI_PT_KEY")
DATA_card_class_count_cc["CARD_COUNT_BY_CLASS_DC"] = DATA_card_class_count_cc["CARD_COUNT_BY_CLASS_DC"].fillna(0).astype(int).astype(str)
DATA_card_class_count_cc["CARD_COUNT_BY_CLASS_CC"] = DATA_card_class_count_cc["CARD_COUNT_BY_CLASS_CC"].fillna(0).astype(int).astype(str)


### """CREATING WAVES BASED ON DATE PROVIDED BY MMB""" ###

DATA_card_class_count_cc["CARD_EXPIRY_DATE"] = pd.to_datetime(DATA_card_class_count_cc['CARD_EXPIRY_DATE'], format='%Y-%m-%d')
DATA_x = DATA_card_class_count_cc 

wave_1 = DATA_x.loc[(DATA_x['CARD_EXPIRY_DATE'] >= '2021-07-01') & (
DATA_exclude['CARD_EXPIRY_DATE'] < '2022-01-01') & (DATA_x["CARD_SEGMENT"].isin(
['MC_Business Gold','MC_Business Other','MC_Business Standard','MC_Retail Gold',
'MC_Retail Gold','MC_Retail Maestro','MC_Retail Other','MC_Retail Standard']))]

wave_2 = DATA_x.loc[(DATA_x['CARD_EXPIRY_DATE'] >= '2022-01-01') & (
DATA_x['CARD_EXPIRY_DATE'] < '2022-10-01') & (DATA_x["CARD_SEGMENT"].isin(
['MC_Business Gold','MC_Business Other','MC_Business Standard','MC_Retail Gold',
'MC_Retail Gold','MC_Retail Maestro','MC_Retail Other','MC_Retail Standard']))]

wave_3 = DATA_x.loc[(DATA_x['CARD_EXPIRY_DATE'] >= '2022-10-01') & (
DATA_x['CARD_EXPIRY_DATE'] < '2024-07-01') & (DATA_x["CARD_SEGMENT"].isin(
['MC_Business Gold','MC_Business Other','MC_Business Standard','MC_Retail Gold',
'MC_Retail Gold','MC_Retail Maestro','MC_Retail Other','MC_Retail Standard']))]

wave_4 = DATA_x.loc[(DATA_x['CARD_EXPIRY_DATE'] >= '2024-07-01') & (
DATA_x['CARD_EXPIRY_DATE'] < '2025-04-01') & (DATA_x["CARD_SEGMENT"].isin(
['MC_Business Gold','MC_Business Other','MC_Business Standard','MC_Retail Gold',
'MC_Retail Gold','MC_Retail Maestro','MC_Retail Other','MC_Retail Standard']))]

wave_5 = DATA_x.loc[(DATA_x['CARD_EXPIRY_DATE'] < '2021-07-01') & (
DATA_x["CARD_SEGMENT"].isin(
['MC_Business Gold','MC_Business Other','MC_Business Standard','MC_Retail Gold',
'MC_Retail Gold','MC_Retail Maestro','MC_Retail Other','MC_Retail Standard']))]

wave_1["WAVE"] = 1
wave_2["WAVE"] = 2
wave_3["WAVE"] = 3
wave_4["WAVE"] = 4
wave_5["WAVE"] = 5

waves = pd.concat([wave_1,wave_2,wave_3,wave_4,wave_5])
waves = waves[['CARD_NUM','WAVE']]

DATA_waves = DATA_x.merge(waves, how="left", on="CARD_NUM")
DATA_waves["WAVE"] = DATA_waves["WAVE"].fillna(0).astype(int).astype(str)

wave_count = DATA_waves.groupby(["UNI_PT_KEY"])["WAVE"].nunique().reset_index()
wave_count.rename(columns={"WAVE":"WAVE_COUNT"}, inplace=True)
DATA_wave_count = DATA_waves.merge(wave_count, how="left", on="UNI_PT_KEY")

### """ADDING BINS FOR TRN AND CURRENT ACCOUNT BALANCE""" ###

bins = [0, 3, 5, 10, 15, 20, 30, 45, 60, 90, 200, 1000, np.inf]
trn_bin_names = [ "1-3", "4-5", "6-10", "11-15", "16-20", "21-30", "31-45", "46-60", "61-90", "91-200", "200-1000", "1000+"]
DATA_wave_count["TRN_BINS"] = pd.cut(DATA_wave_count["TOTAL_TRN_COUNT"], trn_bins, labels=trn_bin_names)
ca_balance_bins = [0, 500, 1000, 5000, 10000, 50000, 100000, 1000000, np.inf]
ca_balance_bin_names= ["0.01-500", "501-1000", "1000-5000", "5001-10000", "10001-50000", "50001-100000", "100001-1000000", "1000000+"]

DATA_wave_count["CA_BAL_BINS"] = pd.cut(DATA_wave_count["CA_BAL"], ca_balance_bins, labels=ca_balance_bin_names)

DATA_wave_count.to_csv("DATA.csv", sep=";", index=False)

