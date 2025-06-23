{{
    config(
        materialized='view'
    )
}}


SELECT
	account_b2b.idibs,
	account_b2b.name,
	account_b2b.consortia1name,
	account_b2b.office,
	account_b2b.billingstate,
	account_b2b.territory,
	account_b2b.Is_Key_Account,
	account_b2b.ponantaccount,
	account_b2b.billingcity,
	account_b2b.billingcountry,
	account_b2b.billingpostalcode,
	account_b2b.iatanumber,
	account_b2b.paymentcondition,
	account_b2b.conditions_de_paiement_pg,
	account_b2b.sales_owner_pg,
	account_b2b.Segment,
	--bkg.bkg_status_code,
	bkg.Booking_Status_Description,
	nvl(bkg.bkg_Calculated_Status_Code,
		CASE WHEN facts.type in ('Erosion Hors IBS','Group And Charter','Group & Charter Free') THEN 'BKD' END) as bkg_Calculated_Status_Code,
	nvl(bkg.Group_ID,facts.gci_group_id) bkg_groupid,
	bkg.bkg_profile,
	bkg.bkg_source,
	bkg.bkg_cancelreason,
	bkg.bkg_departure_date,
	bkg.Booking_Return_Date,
	bkg.departure_city,
	bkg.arrival_city,
	bkg.Rate_Code_Description,
	bkg.Rate_Code,
	bkg.Ponant_Agent_Code,
	bkg.Ponant_Agent_Name,
	bkg.Sponsor,
	bkg.Sponsor_Name,
	bkg.Marketing_Origin,
	bkg.Invoice_Number,
	bkg.bkg_previous_status_code,
	bkg.bkg_traveldocpreference,
	bkg.Travel_Doc_Email_Sent,
	bkg.Travel_Doc_Paper_Sent,
	bkg.bkg_ponantaccount,
	bkg.bkg_fullypaid,
	bkg.bkg_invoiced,
	bkg.bkg_created_date,
	extract ('year' from bkg.bkg_created_date) as Booking_Created_Year,
	extract ('month' from bkg.bkg_created_date) as Booking_Created_Month,
	extract ('week' from bkg.bkg_created_date) as Booking_Created_Week,
	extract ('qtr' from bkg.bkg_created_date) as Booking_Created_Quarter,
	bkg.bkg_history_first_option_date,
	bkg.bkg_history_first_booked_date,
	bkg.booking_confirmation_week,
	bkg.booking_confirmation_month,
	bkg.booking_confirmation_quarter,
	bkg.booking_confirmation_year,
	bkg.Last_Booked_Date,
	bkg.Option_Expiration_Date,
	extract ('year' from bkg.Option_Expiration_Date) as Option_Expiration_Year,
	extract ('month' from bkg.Option_Expiration_Date) as Option_Expiration_Month,
	extract ('week' from bkg.Option_Expiration_Date) as Option_Expiration_Week,
	bkg.Invoice_Date,
	extract ('year' from bkg.Invoice_Date) as Invoice_Year,
	extract ('month' from bkg.Invoice_Date) as Invoice_Month,
	extract ('week' from bkg.Invoice_Date) as Invoice_Week,
	extract ('qtr' from bkg.Invoice_Date) as Invoice_Quarter,
	bkg.bkg_status_code_last_changed_date,
	bkg.Last_Full_Paid_Date,
	bkg.bkg_final_payment_date,
	NVL( CASE WHEN facts.type = 'Group & Charter Free' 
				THEN d_group.gp_group_large END,	
			CASE WHEN cru.mm_cru_Charter = 'Y'
				AND (bkg.bkg_History_Client_Type_1 not in ('DECOME','PRODUIT') 
				OR bkg_History_Client_Type_1 is null) 
				THEN 'CHARTER' end,
				bkg.bkg_History_Client_Type_1,
			CASE WHEN facts.type = 'Group And Charter' 
				THEN 'GROUP' END,	
			CASE WHEN facts.type = 'Erosion Hors IBS' 
				THEN UPPER(facts.ehi_client_type) 
			ELSE null END) as bkg_History_Client_Type_1, 
			NVL(bkg.bkg_direct_agency,
			CASE WHEN facts.type = 'Erosion Hors IBS' AND facts.account_b2b_id = 'ND' THEN 'Direct' END, 
			CASE WHEN facts.type = 'Erosion Hors IBS' AND facts.account_b2b_id <> 'ND' THEN 'Trade' END,
			d_group.grp_direct_agency) as bkg_direct_agency,
			NVL(CASE WHEN cru.mm_cru_Charter = 'Y' THEN
					CASE 
						WHEN cru.mm_cru_Charter_Name like '%A&K%' 
							OR cru.mm_cru_Charter_Name like '%TAUCK%'
							OR cru.mm_cru_Charter_Name like '%GOHAGAN%'
							THEN 'KA' 
						ELSE 'Other'
					END
				ELSE bkg.Account_Type
				END
			,'') as Account_Type,
	bkg.Currency,
	bkg.bkg_office_name,
	nvl(bkg.source_pax_1, d_group.source_pax_1, 'N/A') source_pax_1, 
	nvl(bkg.source_pax_2, CASE WHEN facts.type = 'Erosion Hors IBS' THEN facts.ehi_source_pax_2 ELSE null END , d_group.source_pax_2, 'N/A') as source_pax_2,
	nvl(bkg.source_pax_3, CASE WHEN facts.type = 'Erosion Hors IBS' THEN facts.ehi_source_pax_3 ELSE null END , d_group.source_pax_3, 'N/A') as source_pax_3,
	nvl(bkg.client_id, d_group.client_id, 'N/A') client_id,
	nvl(bkg.client_name, d_group.client_name, 'N/A') client_name,
	nvl(bkg.client_country, d_group.client_country, 'N/A') client_country,
	nvl(bkg.client_region, d_group.client_region, 'N/A') client_region,
	nvl(bkg.client_departement, d_group.client_departement, 'N/A') client_departement,
	nvl(bkg.client_phone, d_group.client_phone, 'N/A') client_phone,
	nvl(bkg.client_mobile, d_group.client_mobile, 'N/A') client_mobile,
	bkg.Bkg_Group_Type,
	bkg.Business_Reference,
	bkg.Business_Report_Name,	
	bkg.bkg_invoiced_amount as bkg_invoiced_amount_dim,
	bkg.bkg_payment_amount as bkg_payment_amount_dim,	
	bkg.bkg_net_balance_due_amount as bkg_net_balance_due_amount_dim,
	case when facts.exchange_rate != 0 then bkg.bkg_invoiced_amount / facts.exchange_rate else null end as bkg_invoiced_amount_eur_dim,
	case when facts.exchange_rate != 0 then bkg.bkg_payment_amount / facts.exchange_rate else null end as bkg_payment_amount_eur_dim,
	case when facts.exchange_rate != 0 then bkg.bkg_net_balance_due_amount / facts.exchange_rate else null end as bkg_net_balance_due_amount_eur_dim,	
	bkg.bkg_booker_profile,
	bkg.bkg_traveller_profile,
	--cru.cru_cruise_code,
	cru.cru_From_To,
	cru.mm_cru_from,
	cru.mm_cru_to,
	cru.cru_cruise_status,
	cru.mm_cru_Departure_Date,
	extract ('month' from cru.mm_cru_Departure_Date) as Departure_Month,
	extract ('week' from cru.mm_cru_Departure_Date) as Departure_Week,
	extract ('qtr' from cru.mm_cru_Departure_Date) as Departure_Quarter,
	nvl(cru.mm_cru_Departure_Year, CASE WHEN facts.type = 'Erosion Hors IBS' THEN cast(facts.ehi_year_departure as integer) ELSE null END ) as mm_cru_Departure_Year,
	nvl(cru.mm_cru_arrival_date,   CASE WHEN facts.type = 'Erosion Hors IBS' THEN facts.ehi_date_departure ELSE null END ) as mm_cru_arrival_date,
	cru.cru_los,
	cru.cru_zone,
	cru.cru_destination_semester,
	cru.cru_Area,
	cru.cru_Product,
	cru.mm_cru_Charter,
	cru.mm_cru_Charter_Name,
	cru.mm_cru_Cruise_Type,
	cru.mm_cru_Theme,
	cru.cru_Guest,
	cru.cru_Partnership,
	cru.mm_cru_Season,
	cru.cru_pre_post_sales_opening_date,
	cru.cru_excu_sales_opening_date,
	cru.Pax_Capacity,	
	cru.Cabin_Capacity,	
	cru.ves_vessel_name,
	cru.Vessel_Short_Name,
	cru.Vessel_Type,
	facts.account_b2b_id,
	facts.person_account_id,
	facts.contact_cp_id,
	facts.booking_id,
	facts.item_id,
	facts.cru_cruise_id as cru_cruise_code,
	facts.type,
	facts.product_type,
	facts.revenue_type,
	facts.revenue_type_description,
	facts.charge_type,
	facts.discount_code,
	facts.discount_description,
	facts.local_currency,
	facts.charge_gross_amount,
	facts.charge_discount_amount,
	(facts.charge_std_commission_amount+facts.charge_bonus_commission_amount) charge_commission_amount,
	facts.charge_net_charge_amount,
	facts.charge_ca_amount,
	facts.charge_gross_amount_eur,
	facts.charge_discount_amount_eur,
	(facts.charge_std_commission_amount_eur+facts.charge_bonus_commission_amount_eur) charge_commission_amount_eur,
	facts.charge_net_charge_amount_eur,
	facts.charge_ca_amount_eur,
	CASE WHEN bkg.bkg_History_Client_Type_1 not in ('DECOME','PRODUIT') THEN facts.charge_discount_non_cruise ELSE 0 END discount_non_cruise,
	CASE WHEN bkg.bkg_History_Client_Type_1 not in ('DECOME','PRODUIT') THEN facts.charge_discount_non_cruise_eur ELSE 0 END discount_non_cruise_eur,
	facts.exchange_rate,
	fx_proforma.exchange_rate as exchange_rate_proforma,
	facts.ehi_amount_allocated as erosion_non_ibs,
	CASE WHEN facts.charge_type = 'OTX' THEN facts.charge_ca_amount ELSE 0 END as gst,
	CASE WHEN facts.charge_type = 'OTX' THEN facts.charge_ca_amount_eur ELSE 0 END as gst_eur,
	CASE WHEN (facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL') THEN facts.charge_ca_amount ELSE 0 END as cancellation_fees,
	CASE WHEN (facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL') THEN facts.charge_ca_amount_eur ELSE 0 END as cancellation_fees_eur,
	CASE WHEN item.bkg_item_type = 'CRU' AND NOT(facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL')
			THEN facts.charge_gross_amount - charge_commission_amount - gst
		ELSE 0 END as net_revenue_cruise,
	CASE WHEN item.bkg_item_type = 'CRU' AND NOT(facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL')
			THEN facts.charge_gross_amount_eur - charge_commission_amount_eur - gst_eur
		ELSE 0 END as net_revenue_cruise_eur,
	CASE 
		WHEN item.bkg_item_type = 'CRU' 		THEN net_revenue_cruise + cancellation_fees - discount_non_cruise
		WHEN facts.type = 'Erosion Hors IBS'	THEN - facts.ehi_amount_allocated
		WHEN facts.type = 'Group And Charter'	THEN facts.gci_ca_inc
		ELSE 0 
		END as ntr_cruise,
	CASE
		WHEN item.bkg_item_type = 'CRU' 		THEN net_revenue_cruise_eur	+ cancellation_fees_eur	- discount_non_cruise_eur
		WHEN facts.type = 'Erosion Hors IBS' 	THEN - facts.ehi_amount_allocated
		WHEN facts.type = 'Group And Charter' AND facts.exchange_rate != 0 THEN facts.gci_ca_inc / facts.exchange_rate		
		ELSE 0 
	END as ntr_cruise_eur,	
	CASE WHEN item.bkg_item_type = 'ACT'    					
			AND item.bkg_item_code like 'ENR%' 						
			AND ((facts.component_category in ('Revenue','Tax') AND bkg.bkg_Calculated_Status_Code not in ('CXL', 'LST') )
				OR (facts.component_category = 'Cancellation Fee'))
            THEN  facts.charge_gross_amount - facts.charge_discount_amount - facts.charge_std_commission_amount  
		ELSE 0 END AS enrichment,
	CASE WHEN item.bkg_item_type = 'ACT'    					
			AND item.bkg_item_code like 'ENR%' 						
			AND ((facts.component_category in ('Revenue','Tax') AND bkg.bkg_Calculated_Status_Code not in ('CXL', 'LST') )
				OR (facts.component_category = 'Cancellation Fee'))
            THEN  facts.charge_gross_amount_eur - facts.charge_discount_amount_eur - facts.charge_std_commission_amount_eur
		ELSE 0 END AS enrichment_eur,
	CASE WHEN facts.charge_type = 'PCH' THEN facts.charge_ca_amount + facts.charge_std_commission_amount ELSE 0 END as port_charges,
	CASE WHEN facts.charge_type = 'PCH' THEN facts.charge_ca_amount_eur + facts.charge_std_commission_amount_eur ELSE 0 END as port_charges_eur,
	facts.category,
	facts.activity,
	facts.sub_activity,
	facts.product_classification,
	facts.product,
	facts.sub_product,	
	facts.gci_nb_cab_fg,
	facts.gci_nb_cab_fb,
	facts.nb_cab as gci_nb_cab_inc,
	facts.nb_cab_paying,
	facts.gci_nb_cab_inc_total,
	facts.gci_nb_pax_fg,
	facts.gci_nb_pax_fb,
	facts.gci_nb_pax_inc_total,
	facts.nb_pax as gci_nb_pax_inc,
	facts.nb_pax_paying,
	facts.gci_ca_fg,
	facts.gci_ca_fb,
	facts.gci_ca_inc_total,
	facts.gci_ca_inc,
	facts.Financial_Account_Base,
	facts.Financial_Account_Discount,
	facts.Financial_Account_Commission,
	fa_base.financial_account_label Financial_Account_Base_label,
	fa_discount.financial_account_label Financial_Account_discount_label,
	fa_commission.financial_account_label Financial_Account_commission_label,
	facts.ehi_type,
	nvl(item.pres_cabin_no,facts.cabin_no) pres_cabin_no,
	nvl(item.bkg_item_status_code,CASE WHEN facts."type" = 'Group And Charter' THEN 'GIP' else null END) bkg_item_status_code,
	nvl(item.pres_cabin_category_code,facts.gci_cabin_category) cabin_category,
	nvl(item.cabin_category_code_equiv , facts.gci_cabin_category) cabin_category_equiv,
	nvl(item.cabin_total_cabin_code_ibs_equiv,CASE WHEN facts."type" in ('Group And Charter','Group & Charter Free')  THEN 1 else null END) cabin_total_cabin_code_ibs_equiv,
	nvl(item.bkg_item_type,CASE WHEN facts."type" = 'Group And Charter' THEN 'CRU' else null END) bkg_item_type,
	item.Cabin_Category_Pricing,
	item.Cabin_Category_Name,
	item.Cabin_Shipboard,
	item.Cabin_Position,
	item.Air_Booking_Type,
	item.bkg_item_code,
	item.Item_Booked_Date,
	item.Item_Fare_Type,
	item.Item_Flight_Class,
	item.Item_Occupancy_Room,
	item.Cabin_Occupancy_Code,
	item.cru_rate_code, 
	item.cru_rate_code_description,
	item.bkg_item_description,
	booking_cruise.pax_max_abt pax_max_abt_total,
	CASE WHEN cru.mm_cru_Charter = 'Y' Then booking_cruise.pax_max_abt Else booking_cruise.pax_max_abt_paying End as pax_max_abt_paying,
	CASE WHEN row_number() over (partition by facts.booking_id, facts.cru_cruise_id ORDER BY fg_cru DESC ) = 1 THEN 1 ELSE 0 END as first_row_booking_cruise,
	booking_cruise.pax_max_cruise * first_row_booking_cruise AS pax_max_cruise_total,
	CASE WHEN cru.mm_cru_Charter = 'Y' 
		Then pax_max_cruise_total 
		Else (booking_cruise.pax_max_cruise_paying * first_row_booking_cruise) 
	End as pax_max_cruise_paying,
	CASE WHEN bkg_status_code in ('BKD', 'OPT','QUO') AND bkg_item_status_code in ( 'BKD', 'OPT','QUO') AND bkg_item_type = 'CRU' THEN 1 ELSE 0 END as fg_pax_cruise, -- fg_pax_cruise
	CASE WHEN bkg.bkg_status_code = 'BKD'				  AND item.bkg_item_status_code = 'BKD' 												THEN 1 ELSE 0 END as fg_pax_abt, -- fg_pax_abt
	CASE WHEN ( facts.charge_type not in ('PCH','OTX') AND NVL(facts.charge_net_charge_amount,0) > 0)
				OR (cru.mm_cru_Charter = 'Y') Then 1 Else 0 End as fg_pax_cruise_paying ,
	CASE WHEN (facts.charge_ca_amount > 0 OR cru.mm_cru_Charter = 'Y') Then 1 Else 0 End as fg_pax_abt_paying, 
	--0 as fg_pax_paying,
	CASE WHEN (facts.charge_ca_amount > 0 OR cru.mm_cru_Charter = 'Y') Then 1 Else 0 End as fg_pax_paying, 
	participant.bkg_fid_completion_date,
	participant.bkg_Passport_completion_Date,
	participant.bkg_QM_Medical_Form_Completion_Date,
	participant.bkg_Air_Request_Completion_Date,
	participant.bkg_fid_reminder_date,
	participant.bkg_Passport_reminder_Date,
	participant.bkg_QM_Medical_Form_reminder_Date,
	participant.bkg_Air_Request_reminder_Date,
	participant.isleadpax,
	person_account.lastname,
	person_account.firstname,
	person_account.usualname,
	person_account.personmailingcountry,
	person_account.personbirthdate,
	person_account.nationality,
	person_account.personmailingpostalcode,
	person_account.personmailingcity,
	person_account.campaignsource,
	person_account.personleadsource,
	person_account.personmailingstate,
	--person_account.complete_phone_channel,
	person_account.hasnoemail,
	person_account.webaccountdate,
	person_account.language,
	person_account.ibsid,
	person_account.Booker_Profile,
	person_account.Traveller_Profile,
	person_account.PYC_Loyalty_Status,
	person_account.ispaulgauguindatabase as Traveller_Is_PG_Only,
	person_account.passportnumber,
	person_account.passportdateofdelivery,
	person_account.passportdateofexpiry,
	person_account.passportplaceofissue,
	row_number() over (partition by facts.booking_id, facts.cru_cruise_id, facts.contact_cp_id, fg_pax_cruise order by facts.charge_gross_amount) as row_num_pcd,
	row_number() over (partition by facts.booking_id, facts.cru_cruise_id, facts.contact_cp_id, fg_pax_cruise, fg_pax_cruise_paying  order by facts.charge_gross_amount) row_num_pcd_paying, 
	row_number() over (partition by facts.booking_id, facts.cru_cruise_id, pres_cabin_no, fg_pax_cruise order by facts.charge_gross_amount) as row_num_ccd,
	row_number() over (partition by facts.booking_id, facts.cru_cruise_id, pres_cabin_no, fg_pax_cruise, fg_pax_cruise_paying order by facts.charge_gross_amount) as row_num_ccd_paying,
	CASE WHEN (first_value(facts.cru_cruise_id)	over ( partition by facts.booking_id order by mm_cru_Departure_Date asc rows between unbounded preceding and unbounded following) = facts.cru_cruise_id OR facts.type = 'Booking Header') THEN 1 ELSE 0 END first_cruise,
	CASE WHEN bkg_item_type = 'CRU' THEN 1 ELSE 0 END AS fg_cru,
	row_number() over (partition by facts.booking_id , facts.cru_cruise_id order by facts.charge_gross_amount) row_num,
	row_number() over (partition by facts.cru_cruise_id order by fg_pax_cruise desc, facts.booking_id asc) row_num_cruise,
	account_b2b.Ownership,
	account_b2b.Consortia2Name,
	account_b2b.Consortia_1_ID,
	account_b2b.REPPrimarycontactEmail__c,
	bkg.bkg_market_name,
	nvl(bkg.Market, d_group.market) market,
	cruise_pax.already_on_board,
	cruise_pax.Staying_On_Board,
	bkg.bkg_conf_date,
	bkg.bkg_cxl_date,
	cru.cruise_modification_date,
	cru.cruise_modification_status,
	bkg.bkg_discount_code,
	case when fx_proforma.exchange_rate != 0 then bkg.bkg_invoiced_amount 					/ fx_proforma.exchange_rate 	else null end as bkg_invoiced_amount_eur_proforma_dim,			
	case when fx_proforma.exchange_rate != 0 then bkg.bkg_payment_amount 					/ fx_proforma.exchange_rate 	else null end as bkg_payment_amount_eur_proforma_dim,		
	case when fx_proforma.exchange_rate != 0 then bkg.bkg_net_balance_due_amount 			/ fx_proforma.exchange_rate 	else null end as bkg_net_balance_due_amount_eur_proforma_dim,	
	case when fx_proforma.exchange_rate != 0 then facts.charge_ca_amount 					/ fx_proforma.exchange_rate 	else null end as charge_ca_amount_eur_proforma, 				
	case when fx_proforma.exchange_rate != 0 then facts.charge_gross_amount 				/ fx_proforma.exchange_rate  	else null end as charge_gross_amount_eur_proforma, 								
	case when fx_proforma.exchange_rate != 0 then facts.charge_std_commission_amount 		/ fx_proforma.exchange_rate  	else null end as charge_std_commission_amount_eur_proforma,
	case when fx_proforma.exchange_rate != 0 then facts.charge_bonus_commission_amount 		/ fx_proforma.exchange_rate   	else null end as charge_bonus_commission_amount_eur_proforma, 
	(charge_std_commission_amount_eur_proforma+charge_bonus_commission_amount_eur_proforma) 						as charge_commission_amount_eur_proforma, 						
	case when fx_proforma.exchange_rate != 0 then facts.charge_discount_amount 		/ fx_proforma.exchange_rate  	else null end as charge_discount_amount_eur_proforma, 			
	case when fx_proforma.exchange_rate != 0 then facts.charge_net_charge_amount 	/ fx_proforma.exchange_rate 	else null end as charge_net_charge_amount_eur_proforma, 		
	case when fx_proforma.exchange_rate != 0 then facts.charge_discount_non_cruise 	/ fx_proforma.exchange_rate 	else null end as charge_discount_non_cruise_eur_proforma, 		
	CASE WHEN bkg.bkg_History_Client_Type_1 not in ('DECOME','PRODUIT') THEN charge_discount_non_cruise_eur_proforma ELSE 0 END discount_non_cruise_eur_proforma,
	CASE WHEN (facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL') THEN charge_ca_amount_eur_proforma ELSE 0 END as cancellation_fees_eur_proforma,					
	CASE WHEN facts.charge_type = 'OTX'	 THEN charge_ca_amount_eur_proforma ELSE 0 END as gst_eur_proforma,																			
	CASE WHEN item.bkg_item_type = 'ACT'    					
			AND item.bkg_item_code like 'ENR%' 						
			AND ((facts.component_category in ('Revenue','Tax') AND bkg.bkg_Calculated_Status_Code not in ('CXL', 'LST') )
				OR (facts.component_category = 'Cancellation Fee'))
			THEN  charge_gross_amount_eur_proforma - charge_discount_amount_eur_proforma - charge_std_commission_amount_eur_proforma
		ELSE 0 END AS enrichment_eur_proforma, 																																		
	CASE WHEN item.bkg_item_type = 'CRU' AND NOT(facts.revenue_type in ('CXT','CXL') OR bkg.bkg_status_code = 'CXL') 
			THEN charge_gross_amount_eur_proforma - charge_commission_amount_eur_proforma - gst_eur_proforma
	ELSE 0 END as net_revenue_cruise_eur_proforma, 																																	
	CASE 
		WHEN item.bkg_item_type = 'CRU' 		THEN net_revenue_cruise_eur_proforma + cancellation_fees_eur_proforma - discount_non_cruise_eur_proforma
		WHEN facts.type = 'Erosion Hors IBS' 	THEN - facts.ehi_amount_allocated
		WHEN facts.type = 'Group And Charter' AND fx_proforma.exchange_rate != 0 THEN facts.gci_ca_inc / fx_proforma.exchange_rate		
		ELSE 0 
	END as ntr_cruise_eur_proforma,																							
	CASE WHEN facts.charge_type = 'PCH' THEN charge_ca_amount_eur_proforma + charge_std_commission_amount_eur_proforma 	ELSE 0 END as port_charges_eur_proforma,
	cru.accd,
	cru.apcd,
	facts.prorata_bkg_shared,
	facts.prorata_bkg_shared_paying
FROM
	 {{ ref('f_facts') }} facts
	LEFT JOIN  {{ ref('d_booking') }} bkg
		ON  facts.booking_id = bkg.booking_ibs_id
	LEFT JOIN {{ ref('d_group') }} d_group 
		ON d_group.group_id = facts.gci_group_id
	LEFT JOIN {{ ref('d_item') }} item
		ON facts.booking_id = item.bkg_booking_no
		AND facts.item_id = item.bkg_item_id
	LEFT JOIN {{ ref('d_cruise') }} cru
		ON facts.cru_cruise_id = cru.cru_cruise_id
	LEFT JOIN {{ ref('d_booking__cruise') }} booking_cruise
		ON facts.booking_id = booking_cruise.bkg_booking_no
		AND cru.cru_cruise_id = booking_cruise.cru_cruise_id
	LEFT JOIN {{ ref('d_booking__cruise__participant') }} cruise_pax
		ON facts.booking_id = cruise_pax.booking_id
		AND cru.cru_cruise_id = cruise_pax.cru_cruise_id
		AND facts.contact_cp_id = cruise_pax.contact_cp_id
	LEFT JOIN {{ ref('d_account_b2b') }} account_b2b
		ON facts.account_b2b_id = account_b2b.account_b2b_id
	LEFT JOIN {{ ref('d_person_account') }} person_account
		ON facts.person_account_id = person_account.person_account_id
	LEFT JOIN {{ ref('d_booking_participant') }} participant
		ON facts.contact_cp_id = participant.bkg_passenger_no
		AND facts.booking_id = participant.bkg_booking_no
	LEFT JOIN {{ ref('d_financial_account') }} fa_base
		ON facts.financial_account_base = fa_base.financial_account_code
	LEFT JOIN {{ ref('d_financial_account') }} fa_discount
		ON facts.financial_account_discount = fa_discount.financial_account_code
	LEFT JOIN {{ ref('d_financial_account') }} fa_commission
		ON facts.financial_account_commission = fa_commission.financial_account_code
	LEFT JOIN {{ ref('d_fx_proforma') }} fx_proforma
		ON facts.local_currency = fx_proforma.currency
WHERE facts.type NOT IN ('Activity','CRM Hierarchy', 'Charge Override', 'ROI Campaign', 'Cabin Deleted', 'Lead Transformation')
