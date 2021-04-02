# Debit card data compilation
import pandas as pd
cols_list = ['UNI_PT_KEY', 'CIF', 'CARD_CLASS_CODE', 'CARD_NUM', 'PRODUCT',
       'PRIMARY_ACCOUNT', 'CARD_SEGMENT', 'CARD_BIN', 'CARD_RANGE', 'EMBLEM_ID',
             'ACCOUNT_OPEN_DATE', 'CARD_ISSUE_DATE', 'CARD_EXPIRY_DATE', 'CARD_ACTIVATION_DATE',
             'FIRST_TRN_DATE', 'CARD_ACT_FLAG','IS_CARD_WITH_TOKEN']

 

debit = pd.read_csv("debitcards.csv", usecols=cols_list, dtype=str, sep=";", error_bad_lines=False, low_memory=False)
a = debit["CARD_NUM"].nunique()
b = debit["UNI_PT_KEY"].nunique()
c = debit["CIF"].nunique()

print("# of UNI_PT_KEY = " +str(b))
print("# of CARD_NUM = " + str(a))
print("# of CIF = " + str(c))

#other products
other_products = pd.read_csv("other_metrics.csv", sep=";", dtype=str)
other_products["OTHER_PRODUCTS"] = 1
dc_other_products = debit.merge(other_products, how="left", on="UNI_PT_KEY")
dc_other_products["OTHER_PRODUCTS"] = dc_other_products["OTHER_PRODUCTS"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_other_products["OTHER_PRODUCTS"].astype(int).sum()))


#mobile banking
mobile_banking = pd.read_csv("mobile_banking.csv", sep=";", dtype=str)
mobile_banking["MOBILE_BANKING"] = 1
mobile_banking = pd.DataFrame(mobile_banking)
dc_mobile_banking = dc_other_products.merge(mobile_banking, how="left", on="UNI_PT_KEY")
dc_mobile_banking["MOBILE_BANKING"] = dc_mobile_banking["MOBILE_BANKING"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_mobile_banking["MOBILE_BANKING"].astype(int).sum()))

    
#internet banking
internet_banking = pd.read_csv("internet_banking.csv", sep=";", dtype=str)
internet_banking["INTERNET_BANKING"] = 1
dc_internet_banking = dc_mobile_banking.merge(internet_banking, how="left", on="UNI_PT_KEY")
dc_internet_banking["INTERNET_BANKING"] = dc_internet_banking["INTERNET_BANKING"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_internet_banking["INTERNET_BANKING"].astype(int).sum()))


#branch delivery
branch_delivery = pd.read_csv("branch_delivery.csv", sep=";", dtype=str)
branch_delivery["BRANCH_DELIVERY"] = 1
dc_branch_delivery = dc_internet_banking.merge(branch_delivery, how="left", on="CARD_NUM")
dc_branch_delivery["BRANCH_DELIVERY"] = dc_branch_delivery["BRANCH_DELIVERY"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_branch_delivery["BRANCH_DELIVERY"].astype(int).sum()))


#staff
staff = pd.read_csv("staff_flag.csv", sep=";", dtype=str)
staff["STAFF_FLAG"] = 1
dc_staff_flag = dc_branch_delivery.merge(staff, how="left", on="UNI_PT_KEY")
dc_staff_flag["STAFF_FLAG"] = dc_staff_flag["STAFF_FLAG"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_staff_flag["STAFF_FLAG"].astype(int).sum()))
 

#email phone
email_phone = pd.read_csv("contact_email_phone.csv", sep=";", dtype=str, error_bad_lines=False, low_memory=False)
dc_email_phone = dc_staff_flag.merge(email_phone, how="left", on ="UNI_PT_KEY")


#contact address
contact_address = pd.read_csv("customer_address.csv", sep=";", dtype=str)
dc_contact_address = dc_email_phone.merge(contact_address, how="left", on="CARD_NUM")
 
 
# owner vs holder
owner_vs_holder = pd.read_csv("card_ownervsholder_dc.csv", sep=";").applymap(str)
dc_owner_flag = dc_contact_address.merge(owner_vs_holder, how="left", on="CARD_NUM")
dc_owner_flag["OWNER_FLAG"] = dc_owner_flag["OWNER_FLAG"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_owner_flag["OWNER_FLAG"].astype(int).sum()))
 

# current balance (run the SQL script again and compare)
current_balance = pd.read_csv("debit_current_balance.csv", sep=";", low_memory=False, error_bad_lines=False)
current_balance["SRC_ID"] = current_balance["SRC_ID"].astype(int).astype(str)
current_balance["CA_BAL"] = current_balance["CA_BAL"].apply(lambda x: x.replace(",", ".") if isinstance(x, str) else x).astype(str)
current_balance.drop_duplicates(subset="SRC_ID", keep="first", inplace=True)

 

dc_current_balance = dc_owner_flag.merge(current_balance, how="left", left_on="PRIMARY_ACCOUNT", right_on="SRC_ID")

dc_current_balance.drop("SRC_ID", axis=1, inplace=True)
 
del(current_balance, dc_owner_flag, owner_vs_holder, contact_address, email_phone, staff, branch_delivery, internet_banking,
mobile_banking, other_products, dc_contact_address, dc_email_phone, dc_staff_flag, dc_branch_delivery, dc_internet_banking, dc_mobile_banking, dc_other_products,debit)
 

# insurance
cols_list = ["CARD_NUM", "INSURANCE_FLAG"]
insurance_flag = pd.read_csv("16_dc_insurance.csv", sep=";", usecols=cols_list).applymap(str)
insurance_flag["INSURANCE_FLAG"] = 1
dc_insurance_flag = dc_current_balance.merge(insurance_flag, how="left", on="CARD_NUM")
dc_insurance_flag["INSURANCE_FLAG"] = dc_insurance_flag["INSURANCE_FLAG"].fillna(0).astype(int).astype(str)
print("matched records = " + str(dc_insurance_flag["INSURANCE_FLAG"].astype(int).sum()))


#transactions
transactions = pd.read_csv("transactions_grouped_all_TRNTP.csv", sep=";", dtype=str, error_bad_lines=False)


def clean_trn_amt(x):
    if isinstance(x, str):
        return(x.replace(",", "."))
    return(x)

transactions["TRN_AMT"] = transactions["TRN_AMT"].apply(clean_trn_amt).astype(float).round(2)
transactions["TRN_COUNT"] = transactions["TRN_COUNT"].astype(int)

total_transactions = transactions.groupby(["CARD_NUM"])["TRN_COUNT", "TRN_AMT"].sum().reset_index()
total_transactions.rename(columns = {"TRN_COUNT": 'TOTAL_TRN_COUNT', "TRN_AMT":'TOTAL_TRN_AMT'}, inplace=True)

pos_trn = transactions[(transactions["TRN_TP"]== "01_pos_trn")]
pos_trn.rename(columns = {"TRN_COUNT":'POS_TRN_COUNT', "TRN_AMT":'POS_TRN_AMT'}, inplace=True)
pos_trn.drop("TRN_TP", axis=1, inplace=True)

atm_trn = transactions[(transactions["TRN_TP"]=="02_atm_trn")]
atm_trn.rename(columns = {"TRN_COUNT":'ATM_TRN_COUNT', "TRN_AMT":'ATM_TRN_AMT'}, inplace=True)
atm_trn.drop("TRN_TP", axis=1, inplace=True)

net_trn = transactions[(transactions["TRN_TP"]=="03_net_trn")]
net_trn.rename(columns = {"TRN_COUNT":'NET_TRN_COUNT', "TRN_AMT":'NET_TRN_AMT'}, inplace=True)
net_trn.drop("TRN_TP", axis=1, inplace=True)

order_trn = transactions[(transactions["TRN_TP"]=="04_order_trn")]
order_trn.rename(columns = {"TRN_COUNT":'ORDER_TRN_COUNT', "TRN_AMT":'ORDER_TRN_AMT'}, inplace=True)
order_trn.drop("TRN_TP", axis=1, inplace=True)


dc_total_trn = dc_insurance_flag.merge(total_transactions, how="left", on="CARD_NUM")
dc_total_trn["TOTAL_TRN_COUNT"] = dc_total_trn["TOTAL_TRN_COUNT"].fillna(0).astype(int)
dc_total_trn["TOTAL_TRN_AMT"] = dc_total_trn["TOTAL_TRN_AMT"].fillna(0).astype(int).astype(str)

dc_pos_trn = dc_total_trn.merge(pos_trn, how="left", on="CARD_NUM")
dc_pos_trn["POS_TRN_COUNT"] = dc_pos_trn["POS_TRN_COUNT"].fillna(0).astype(int)
dc_pos_trn["POS_TRN_AMT"] = dc_pos_trn["POS_TRN_AMT"].fillna(0).astype(int).astype(str)

dc_atm_trn = dc_pos_trn.merge(atm_trn, how="left", on="CARD_NUM")
dc_atm_trn["ATM_TRN_COUNT"] = dc_atm_trn["ATM_TRN_COUNT"].fillna(0).astype(int)
dc_atm_trn["ATM_TRN_AMT"] = dc_atm_trn["ATM_TRN_AMT"].fillna(0).astype(int).astype(str)

dc_net_trn = dc_atm_trn.merge(net_trn, how="left", on="CARD_NUM")
dc_net_trn["NET_TRN_COUNT"] = dc_net_trn["NET_TRN_COUNT"].fillna(0).astype(int)
dc_net_trn["NET_TRN_AMT"] = dc_net_trn["NET_TRN_AMT"].fillna(0).astype(int).astype(str)

dc_order_trn = dc_net_trn.merge(order_trn, how="left", on="CARD_NUM")
dc_order_trn["ORDER_TRN_COUNT"] = dc_order_trn["ORDER_TRN_COUNT"].fillna(0).astype(int)
dc_order_trn["ORDER_TRN_AMT"] = dc_order_trn["ORDER_TRN_AMT"].fillna(0).astype(int).astype(str)


del( pos_trn, net_trn, order_trn, atm_trn, dc_insurance_flag, insurance_flag, dc_current_balance)
# continue dc_net_trn
 
#avg balance dc
cols_list = ["SRC_ID", "AVG_DR_BAL"]
avg_balance = pd.read_csv("avgmonthlybalance_dc.csv", sep=";", usecols=cols_list,  dtype=str, low_memory=False, error_bad_lines=False)
avg_balance["AVG_DR_BAL"] = avg_balance["AVG_DR_BAL"].apply(lambda x: x.replace(",", ".") if isinstance(x, str) else x).astype(float).astype(int).astype(str)
avg_balance.drop_duplicates(subset="SRC_ID", keep="first", inplace=True)
dc_avg_bal = dc_order_trn.merge(avg_balance, how="left", left_on="PRIMARY_ACCOUNT", right_on="SRC_ID")
sum(pd.isnull(dc_avg_bal["SRC_ID"]))
 
DC_DATA = dc_avg_bal

#reorganized
DC_DATA = DC_DATA[
['UNI_PT_KEY', 'CIF', 'CARD_CLASS_CODE', 'CARD_NUM', 'PRIMARY_ACCOUNT', 'PRODUCT', 'CARD_SEGMENT', 'CARD_BIN', 'CARD_RANGE', 'EMBLEM_ID', 'SRC_ID', 'CURR_ACC_ID',
'ACCOUNT_OPEN_DATE', 'CARD_ISSUE_DATE', 'CARD_EXPIRY_DATE', 'CARD_ACTIVATION_DATE', 'FIRST_TRN_DATE',
'CARD_ACT_FLAG','IS_CARD_WITH_TOKEN', 'OTHER_PRODUCTS', 'MOBILE_BANKING','INTERNET_BANKING', 'BRANCH_DELIVERY', 'STAFF_FLAG','OWNER_FLAG',  'INSURANCE_FLAG',
'MOBILE_PHONE_NUM','CONT_EMAIL_1', 'CONT_EMAIL_2', 'CONT_EMAIL_3', 'CONT_EMAIL_ADDR', 'STREET_NAME', 'STREET_NUM', 'CITY_NAME', 'ZIP',
'TOTAL_TRN_COUNT', 'TOTAL_TRN_AMT', 'POS_TRN_COUNT', 'POS_TRN_AMT', 'ATM_TRN_COUNT', 'ATM_TRN_AMT', 'NET_TRN_COUNT', 'NET_TRN_AMT','ORDER_TRN_COUNT', 'ORDER_TRN_AMT',
 'CA_BAL', 'AVG_DR_BAL']
]
DC_DATA.to_csv("DC_DATA.csv", index=False, sep=";")



































