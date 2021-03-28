--------------------------------------------------------------------------------------------------SAMPLE SCRIPT -------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------ORDERS----------------------------------------------------------------------------------------------------

create or replace table "risk_view" as 
select distinct CURRENT_DATE() as "rep_date",
       ord."id" as "order_ID",
       ord."customer_id" as "cust_ID",
       ord."type",
       case
        when ord."type" = '1' then 'New'
        when ord."type" = '2' then 'Existing'
        else null
       end as "cust_type",
       mig2."migrated_from_HC",
       ord."created_at",
       try_cast(ord."created_at" as DATE) as "created_date",
       year(try_cast(ord."created_at" as TIMESTAMP_NTZ)) as "created_year",
       month(try_cast(ord."created_at" as TIMESTAMP_NTZ)) as "created_month",
       week(try_cast(ord."created_at" as TIMESTAMP_NTZ)) as "created_week",
       dayname(try_cast(ord."created_at" as TIMESTAMP_NTZ)) as "created_day",
       hour(try_cast(ord."created_at" as TIMESTAMP_NTZ)) as "created_hour",
       cb_ord_state."Finished" as "finished",
       ord."name",
       ord."first_name",
       ord."last_name",
       ord."before_degree",
       ord."after_degree",
       cust."birth_date",
       cust."personal_id" as "cust_RC",
       ord."tin" as "cust_IC",
       cust."id_card_no",
       cust."id_card_expiry_date",
       ord."email",
       ord."phone",
       ord."ip_address",
       fp."local_ips",
       fp."footprint",
       replace(fp."user_agent", ';', '|') as "user_agent",
       fp."cookie",
       fp."resolution",
       fp."social_leaks",
       cust."is_blocked",
       cust."is_emergency",
       cust."is_required_id_card",
       ord."eshop_id",
       eshp."name" as "eshop_name",
       extr2."eshop_cust_type",
       extr2."transaction_count",
       extr2."cashless_count",
       extr2."total_spending",
       extr2."earliest_transaction",
       extr2."latest_transaction",
       item."items_count" as "main_items_count",
       to_number(parse_json(scr."credit_result"): "main_items_price"."value") as "main_items_price",
       to_number(parse_json(scr."credit_result"): "isir_debtors_100m"."value") as "isir_debtors_100m",
       to_varchar(parse_json(scr."fraud_result"): "email_domain_detail"."value") as "email_domain_detail",
       to_varchar(parse_json(scr."credit_result"): "ua_pc_tabl_mob_bot"."name") as "ua_pc_tabl_mob_bot",
       ifnull(to_varchar(parse_json(scr."credit_result"): "order_time"."value"), to_varchar(parse_json(scr."fraud_result"): "order_time"."value")) as "order_time",
       ifnull(to_number(parse_json(scr."credit_result"): "suspicious_surname_count"."value"), to_number(parse_json(scr."credit_result"): "suspicious_surname_cnt"."value")) as "suspicious_surname_count",
       ifnull(to_boolean(parse_json(scr."credit_result"): "numbers_in_email_flag"."value"), to_boolean(parse_json(scr."credit_result"): "numbers_in_email_sign"."value")) as "numbers_in_email_flag",
       ifnull(to_boolean(parse_json(scr."credit_result"): "capitalized_name"."value"), to_boolean(parse_json(scr."credit_result"): "name_caps_case"."value")) as "capitalized_name",
       to_number(parse_json(scr."credit_result"): "ld_name_email"."value") as "ld_name_email",
       to_number(parse_json(scr."credit_result"): "isir_050m_cls050m05d"."value") as "isir_050m_cls050m05d",
       to_boolean(parse_json(scr."credit_result"): "numbers_in_email_sign"."value") as "numbers_in_email_sign",
       to_boolean(parse_json(scr."credit_result"): "name_caps_case"."value") as "name_caps_case",
       to_number(parse_json(scr."credit_result"): "suspicious_surname_cnt"."value") as "suspicious_surname_cnt",
       to_number(parse_json(scr."credit_result"): "screen_resolution"."value") as "screen_resolution",
       to_number(parse_json(scr."credit_result"): "main_items"."value") as "main_items",
       to_number(parse_json(scr."credit_result"): "amount_main_items"."value") as "amount_main_items",
       to_varchar(parse_json(scr."fraud_result"): "expencatg_category_first"."value") as "expencatg_category_first",
       to_number(parse_json(scr."credit_result"): "geoskoring_1"."value") as "geoskoring_1",
       to_number(parse_json(scr."fraud_result"): "geoskoring_2"."value") as "geoskoring_2",
       to_number(parse_json(scr."credit_result"): "cnt_order_0100m07d"."value") as "cnt_order_0100m07d",
       scr."credit_score" as "credit_PD",
       scr."category_slug" as "credit_rating",
       scr."fraud_score" as "fraud_PD",
       case
        when scr."fraud_score" is null then null
        when try_cast(scr."fraud_score" as double) < '0.0362794291' then 'A'
        when try_cast(scr."fraud_score" as double) < '0.0738265435' then 'B'
        when try_cast(scr."fraud_score" as double) < '0.12' then 'C'
        when try_cast(scr."fraud_score" as double) <= '1' then 'D'
        else null
       end as "fraud_rating",
       pts."points",
       pts."price_limit" as "limit",
       pts."available_limit",
       telscr."telco_consent",
       telscr."telco_score",
       geoscr."geo_segment",
       geoscr."geo_PD",
       geoscr."geo_ER",
       geoscr."is_gov_office",
       geoscr."income_decile",
       ord."state",
       cb_ord_state."state_desc_EN" as "state_desc",
       ord."state_reason",
       cb_ord_state_reason."state_reason_desc_EN" as "state_reason_desc",
       ord."total_price",
       item."main_item_name",
       replace(split_part (item."main_item_category_tree",',',1),'{') as "main_item_cat_1",
       replace(split_part (item."main_item_category_tree",',',2),'}') as "main_item_cat_2",
       replace(split_part (item."main_item_category_tree",',',3),'}') as "main_item_cat_3",
       replace(split_part (item."main_item_category_tree",',',4),'}') as "main_item_cat_4",
       replace(split_part (item."main_item_category_tree",',',5),'}') as "main_item_cat_5",
       item."main_item_price",
       ba."zip_code" as "ba_zip_code",
       ba."normalized_city" as "ba_city",
       replace(ba."normalized_full_street",';','') as "ba_street",
       da."zip_code" as "da_zip_code",
       da."normalized_city" as "da_city",
       replace(da."normalized_full_street",';','') as "da_street",
       valset1."state_desc_EN" as "valresult_customer_created",
       valset2."state_desc_EN" as "valresult_first_order",
       valset3."state_desc_EN" as "valresult_second_order",
       valset2."state_desc_EN" as "valresult_ID_card",
       valset2."state_desc_EN" as "valresult_personal_id",
       ord."due_date",
       ord."debt",
       ord."real_customer_debt" as "customer_debt",
       ord."payment_date",
       ord."payment_state",
       ord."customer_payment_state",
       ord."debt_state",
       cb_debt_state."debt_state_CZ",
       case
        when ord."payment_state" = '2' then IFF(DATEDIFF(day,try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))<0,'0',DATEDIFF(day,try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date)))
        else IFF(datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())<0, '0', datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE()))
       end as DPD,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 1 then '1'
        else '0'
       end as DPD1,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 4 then '1'
        else '0'
       end as DPD4,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 10 then '1'
        else '0'
       end as DPD10,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 20 then '1'
        else '0'
       end as DPD20,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 30 then '1'
        else '0'
       end as DPD30,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 60 then '1'
        else '0'
       end as DPD60,
       case
        when (case
            when ord."payment_state" = '2' then DATEDIFF(day, try_cast(ord."due_date" as date), try_cast(ord."payment_date" as date))
            else datediff(day,try_cast(ord."due_date" as date),CURRENT_DATE())
           end) >= 90 then '1'
        else '0'
       end as DPD90
       
from "commerce-order" as ord
left join "customers-customer" as cust on ord."customer_id" = cust."user_ptr_id"
-- eshops names
left join "commerce-eshop" as eshp on ord."eshop_id" = eshp."id"
-- billing address from order
left join "commerce-orderaddress" as ba on ord."id" = ba."order_id" and ba."type" = 1
-- delivery address from order
left join "commerce-orderaddress" as da on ord."id" = da."order_id" and da."type" = 2
-- eshop extra data
left join 
    (select extr."customer_id",
            case
                when sum(to_number(parse_json(extr."data"): "transaction_history"."total_price",10,2))>0 then 'existing'
                else 'new'
            end as "eshop_cust_type",
            sum(extr."transaction_count") as "transaction_count",
            sum(extr."cashless_count") as "cashless_count",
            sum(to_number(parse_json(extr."data"): "transaction_history"."total_price",10,2)) as "total_spending",
            min(to_date(parse_json(extr."data"): "transaction_history"."earliest_transaction")) as "earliest_transaction",
            max(to_date(parse_json(extr."data"): "transaction_history"."latest_transaction")) as "latest_transaction"
    from "customers-customerexternalverificationbonus" as extr
    where contains(extr."data", 'latest_transaction')
    group by extr."customer_id") as extr2
on ord."customer_id" = extr2."customer_id"
-- items
left join
    (select distinct itm."order_id",
            count(itm."id") over(partition by itm."order_id") as "items_count",
            replace(first_value(itm."name") over(partition by itm."order_id" order by try_cast(itm."total_price" as decimal) desc), ';', '|') as "main_item_name",
            replace(first_value(itm."heureka_categories_tree") over(partition by itm."order_id" order by try_cast(itm."total_price" as decimal) desc), ';', '|') as "main_item_category_tree",
            max(try_cast(itm."total_price" as decimal)) over(partition by itm."order_id") as "main_item_price"
     from "commerce-orderitem" as itm
     where not(regexp_like(itm."code", '([0-9]{5})|([0-9]{2})|(.DAR.*)|(.AKCE.*)|(.*KUPNAJISTO.*)|(.*LYMET.*)|(.*kupnajisto.*)|(.*lymet.*)|(S2P.*)|(.*MALL.*)')
        or itm."code" in ('POSTOVNE', 'POJISTNE', 'DOPRAVA', 'S2OSOBNE', 'SLEVA', 'DELIVERY', 'BALNE', 'PRIPLATKY', '110054'))) as item
on ord."id" = item."order_id"
-- credit and fraud score
left join "customers-customercreditbonus" as scr on ord."customer_id" = scr."customer_id"
-- telco score
left join
    (select ts1."order_id",
            case
               when ts2."validator_slug" = 'phone-04'and to_double(parse_json(ts2."computed_attrs"): "telco_score") is null then 'No'
               when ts2."validator_slug" = 'phone-04'and to_double(parse_json(ts2."computed_attrs"): "telco_score") is not null then 'Yes'
               else null
            end as "telco_consent",
            to_double(parse_json(ts2."computed_attrs"): "telco_score") as "telco_score"
     from "predator-validatorsetresult" as ts1
     left join "predator-validatorresult" as ts2 on ts1."id" = ts2."validator_set_result_id"
     where is_decimal(try_to_decimal(ts1."order_id")) and ts1."state"<>5 and ts2."validator_slug" like 'phone-04' and ts2."validator_config_id" in ('91','92')) as telscr
on ord."id" = telscr."order_id"
-- GeoApply
left join
    (select distinct gs1."order_id",
            ifnull(to_varchar(parse_json(gs2."computed_attrs"): "geoapply_response"."geo_segmentation"), to_varchar(parse_json(gs2."computed_attrs"): "geo_apply_response"."geo_segmentation")) as "geo_segment",
            ifnull(to_number(parse_json(gs2."computed_attrs"): "geoapply_response"."probability_of_default"), to_number(parse_json(gs2."computed_attrs"): "geo_apply_response"."probability_of_default")) as "geo_PD",
            ifnull(to_number(parse_json(gs2."computed_attrs"): "geoapply_response"."rate_of_enforceability")/100, to_number(parse_json(gs2."computed_attrs"): "geo_apply_response"."rate_of_enforceability")/100) as "geo_ER",
            ifnull(parse_json(gs2."computed_attrs"): "geoapply_response"."is_government_office_address", parse_json(gs2."computed_attrs"): "geo_apply_response"."is_government_office_address") as "is_gov_office",
            ifnull(to_number(parse_json(gs2."computed_attrs"): "geoapply_response"."income_decile"), to_number(parse_json(gs2."computed_attrs"): "geo_apply_response"."income_decile")) as "income_decile"
     from "predator-validatorsetresult" as gs1
     left join "predator-validatorresult" as gs2 on gs1."id" = gs2."validator_set_result_id"
     where is_decimal(try_to_decimal(gs1."order_id")) and gs1."state"<>5 and gs2."result"<>'8' and gs2."validator_slug" like 'address-06'and gs2."computed_attrs" not like '{}') as geoscr
on ord."id" = geoscr."order_id"
-- validator set result - CUSTOMER_CREATED
left join 
    (select vs1."order_id", vsr1."state_desc_EN"
     from "predator-validatorsetresult" as vs1
     left join "cb_validatorset_result" as vsr1 on vs1."state" = vsr1."state"
     where vs1."validator_set_slug" = 'CUSTOMER_CREATED' and is_decimal(try_to_decimal(vs1."order_id")) and vs1."state"<>5) as valset1
on ord."id" = valset1."order_id"
-- validator set result - FIRST_ORDER_APPROVING
left join 
    (select vs2."order_id", vsr2."state_desc_EN"
     from "predator-validatorsetresult" as vs2
     left join "cb_validatorset_result" as vsr2 on vs2."state" = vsr2."state"
     where vs2."validator_set_slug" = 'FIRST_ORDER_APPROVING' and is_decimal(try_to_decimal(vs2."order_id")) and vs2."state"<>5) as valset2
on ord."id" = valset2."order_id"
-- validator set result - SECOND_ORDER_APPROVING
left join 
    (select vs3."order_id", vsr3."state_desc_EN"
     from "predator-validatorsetresult" as vs3
     left join "cb_validatorset_result" as vsr3 on vs3."state" = vsr3."state"
     where vs3."validator_set_slug" = 'SECOND_ORDER_APPROVING' and is_decimal(try_to_decimal(vs3."order_id")) and vs3."state"<>5) as valset3
on ord."id" = valset3."order_id"
-- validator set result - ADDED_ID_CARD_DATA
left join 
    (select vs4."order_id", vsr4."state_desc_EN"
     from "predator-validatorsetresult" as vs4
     left join "cb_validatorset_result" as vsr4 on vs4."state" = vsr4."state"
     where vs4."validator_set_slug" = 'ADDED_ID_CARD_DATA' and is_decimal(try_to_decimal(vs4."order_id")) and vs4."state"<>5) as valset4
on ord."id" = valset4."order_id"
-- validator set result - ADDED_PERSONAL_ID
left join 
    (select vs5."order_id", vsr5."state_desc_EN"
     from "predator-validatorsetresult" as vs5
     left join "cb_validatorset_result" as vsr5 on vs5."state" = vsr5."state"
     where vs5."validator_set_slug" = 'ADDED_PERSONAL_ID' and is_decimal(try_to_decimal(vs5."order_id")) and vs5."state"<>5) as valset5
on ord."id" = valset5."order_id"
-- points and limit at the time of the order
left join
    (select distinct
            pts2."customer_id",
            pts2."CREATED_DATE",
            pts3."points",
            pts3."price_limit",
            pts3."available_limit"
     from (select pts1."customer_id",
                 try_cast(pts1."created_at" as DATE) as created_date,
                 max(pts1."id") OVER ( PARTITION BY pts1."customer_id", try_cast(pts1."created_at" as DATE) ORDER BY try_cast(pts1."created_at" as DATE)) as max_id
          from "customers-pointschange" as pts1) as pts2
     left join "customers-pointschange" as pts3 on pts2."customer_id" = pts3."customer_id" and pts2."MAX_ID" = pts3."id") as pts
on ord."customer_id" = pts."customer_id" and try_cast(ord."created_at" as DATE) = pts."CREATED_DATE"
-- migration from HC
left join
    (select mig1."customer_id", '1' as "migrated_from_HC"
     from "customers-pointschange" as mig1
     where mig1."description" like 'Migrace zákazníka ze staré aplikace HC Lymet') as mig2
on ord."customer_id" = mig2."customer_id"
-- Footprint and Social Leaks
left join
    (select f1."id" as "fingerprint_id", f1."value" as "footprint", f2.*
       from "fingerprint-computedfingerprint" as f1
  left join "fingerprint-fingerprintdata" as f2 on f2."id" = f1."source_data_id") as fp
on try_cast(ord."fingerprint_id" as number) = fp."fingerprint_id"
 
-- order state description
left join "cb_order_state" as cb_ord_state on ord."state" = cb_ord_state."state"
-- order state reason description
left join "cb_order_state_reason" as cb_ord_state_reason on ord."state_reason" = cb_ord_state_reason."state_reason"
-- delivery type description
--left join "cb_delivery_type" as cb_deliv_type on ord."delivery_type" = cb_deliv_type."delivery_type"
-- delivery carrier description
--left join "cb_delivery_carrier" as cb_deliv_carrier on ord."delivery_carrier" = cb_deliv_carrier."delivery_carrier"
-- debt state description
left join "cb_debt_state" as cb_debt_state on ord."debt_state" = cb_debt_state."debt_state"
where ord."eshop_id"<>'1';
*/
-- Validators view based on table predator-validatorresult ---------------------------------------------------------------------------
create or replace table "risk_validators" as
select CURRENT_DATE() as "rep_date",
   case
         when is_decimal(try_to_decimal(valset1."order_id")) then eshp_o."name"
         when is_decimal(try_to_decimal(valset1."precheck_id")) then eshp_p."name"
         else null
       end as "eshop_name",
   case
         when is_decimal(try_to_decimal(valset1."order_id")) then try_to_decimal(valset1."customer_id", 38, 0)
         when is_decimal(try_to_decimal(valset1."precheck_id")) then try_to_decimal(prch."matched_customer_id", 38, 0)
         else null
       end as "customer_id",
       case
         when is_decimal(try_to_decimal(valset1."order_id")) then 'order'
         when is_decimal(try_to_decimal(valset1."precheck_id")) then 'precheck'
         else null
       end as "run",
       valset1."order_id",
       valset1."precheck_id",
       valset1."id" as "valset_id",
       valset1."validator_set_id" as "valset_desc",
       valset1."created_at",
       valset1."state" as "valset_result_code",
       valsetresult."state_desc_EN" as "valset_result_desc",
       val1."validator_slug" as "validator",
       val1."validator_config_id",
       valconf."title",
       val1."result" as "val_result_code",
       valresult."result_desc_EN" as "val_result_desc",
       val1."fail_action",
       valfail."fail_desc_EN" as "fail_action_desc",
       val1."computed_attrs",
       val1."message"

from "predator-validatorsetresult" as valset1
left join "predator-validatorresult" as val1 on valset1."id" = val1."validator_set_result_id"
left join "predator-validatorconfig" as valconf on val1."validator_config_id" = valconf."id"
left join "cb_validator_result" as valresult on val1."result" = valresult."result"
left join "cb_validatorset_result" as valsetresult on valset1."state" = valsetresult."state"
left join "commerce-order" as ord on valset1."order_id" = ord."id"
left join "predator-precheck" as prch on try_to_number(valset1."precheck_id") = prch."id"
left join "commerce-eshop" as eshp_o on ord."eshop_id" = eshp_o."id"
left join "commerce-eshop" as eshp_p on prch."eshop_id" = eshp_p."id"
left join "cb_validatorconfig_fail" as valfail on valfail."failed_action" = val1."fail_action"
where (ord."eshop_id" <> '1' or ord."eshop_id" is null) and (prch."eshop_id" <> '1' or prch."eshop_id" is null)
;
