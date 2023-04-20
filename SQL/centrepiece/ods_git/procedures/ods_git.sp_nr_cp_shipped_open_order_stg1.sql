CREATE OR REPLACE PROCEDURE ods_git.sp_nr_cp_shipped_open_order_stg1(out return_val int4)
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$
	
	
	
	
	
	
/*
********************************************************
**********************************************************************************************
Object Name  : sp_nr_cp_shipped_open_order_stg1
Creation date : 30-03-2020
Description      : This Function will insert data on full load into nr_shipped_open_order_stg1 table. 
Revision History:
---------------------
Date       Version    Updated by        Comments
-----------------------------------------------------------------------------------------------------------------------------------------------------------
Mar30,2020	1.0       Ankit Anurag      This Job will insert data into xrnr_cp_shipped_open_order table 
May25,2020  2.0		  Hajaran.v			Updating columns to show data for null values.
May25,2020  2.1		  Ankit Anurag		updating code to remove an extra filter from ods_git.shipOpen_oola_tmp
Jul09,2020	2.2		  Ankit Anurag		Creating persistent layer table as temp table is leading to SSL error in QA
Jul10,2020  2.3       Pankaj Singh		Changing query based on new changes in sql
Jul14,2020  2.4       Pankaj Singh		Changing logic for net_sales and 
Jul15,2020  2.5       Pankaj Singh		Changing logic of RSP Concession
Sep18,2020	2.6		  Pankaj Singh		Changing logic for shop cost
Oct01,2020	2.7		  Parth Kumar		Changing logic for Fiscal Month, GL Ship date, Quarter & Year
Oct22,2020	2.8		  Parth Kumar		Changing RMA Logic Change 
Nov23,2020	2.9		  Parth Kumar		Changing RMA Logic for ShopCost Column
Mar31,2021	3.0		  Parth Kumar		Change implemented for Shop Cost, RSP Sales & RSP Concessions
Apr14,2021	3.1		  Ankit Anurag		Logic modification for columms - qty,sales,concessions,concessions_discount,net_sales,shop_cost,
										rsp_sales,rsp_concessions,cm ,cm_per ,other_cost
Aug15,2021	4.1		  Parth Kumar		Changes based on ShippedVsOpen_Report_09Aug2021_Modified.sql
Aug25,2021	5.1		  Parth Kumar		Code Optimization based on line item Logic
Sep03,2021	5.2		  Parth Kumar		Change in (DF) table & extra conditon added in "SHOP_COST"			
Sep13,2021  5.3		  Ankit Anurag		logic fix for ship_date , aircraft_number, line_number, product_type filter
Oct10,2021	5.4		  Parth Kumar		1) Addition of column "aircraft_delivery_date"
										2) Logic Change for RSP Concession & ShopCost Column
Nov09,  2021  5.5	  Parth Kumar		Fix for Year, Quarter & Month (from ship_date column) 						  	
Jan28 	2022  5.6	  Sindhu Palagiri	Logic change for column ‘concessions_ap_misc ’ 
Feb21 	2022  5.7	  Sindhu Palagiri	Logic change for column ‘concessions_ap_misc ’  				  	
Mar14 	2022  5.8	  Sindhu Palagiri	CF6_Enhancement:Logic change for column ‘concessions_discount ’  				  	
May16 	2022  5.9	  Johnson Chu		re-enabled SHIP_DATE logic for GEAE_CEO_ORDER_TYPE for 16May2022.sql update 
May19 	2022  6.0	  Johnson Chu       Adding new column (order.po_ship_date) from 16May2022.sql update
July06  2022  6.1	  Sindhu Palagiri	Excluding SPEC Orders 
July19  2022  6.2     Sindhu Palagiri   Modified Columns  Ship date(Fiscal Month, Quarter & Year), rsp_sales,rsp_concessions
July28  2022  6.3     Sindhu Palagiri   Added Column 'Invoice_Date'
Sep05,  2022  6.4	  Sindhu Palagiri   Addition of Union 2 'Misc AR' & Union 3 'Misc AP' to split the records of AP,AR.
Jan09,  2023  6.5	  Sindhu Palagiri	Modified Columns RSP_SALES, RSP_CONCESSIONS	--V6.5
Feb08,  2023  6.6     Sindhu Palagiri   Modified Column Shop_cost, hou.name to include Passport data from lookup.--V6.6
Mar08,  2023  6.7	  Sindhu Palagiri   Added new column 'mc_segment2_str' in tmp table 'tmp_mtl_5table_set1_sor' to accept negative substring length --V6.7
-----------------------------------------------------------------------------------------------------------------------------------------------------------
Mar15,	2023  7		  Prashanth/Naveen	Converted to RS	
*****************************************************************************************************************************************************
*/
declare
vstatus varchar(256);
vcnt integer;
vdate timestamp := getdate();
venddate timestamp;
v_ge_ceo_org 			INTEGER ;
v_ceo_peb_org			Integer;
begin

call ods_git.sp_nr_procedure_run_log('sp_nr_cp_shipped_open_order_stg1',vstatus,'nr_cp_shipped_open_order_stg1',1,'','','F');


if vstatus = 'RUNNING' then
raise exception 'Job is in RUNNING status';
return;
end if;

drop table if exists orgid_tmp;
create temp table orgid_tmp as	select organization_id --into vorgid 
                  from sc_ods_evncpad1.cp_mv.org_organization_definitions
                 where organization_name = 'CEO - Peebles';
                
select hou.organization_id into  v_ge_ceo_org 
from sc_ods_evncpad1.cp_mv.hr_operating_units hou 
	where hou.name='GE Commercial Engines Organization';                 
                
--with FINAL_TAB as
                drop table if exists ods_git.nr_shipped_open_order_final_table_stg;
CREATE TABLE IF NOT EXISTS ods_git.nr_shipped_open_order_final_table_stg
(
	--segment3 VARCHAR(120)   ENCODE lzo
	 segment1 VARCHAR(120)   ENCODE lzo
	,description VARCHAR(720)   ENCODE lzo
	,msib_inventory_item_id NUMERIC(38,4)   ENCODE az64
	,organization_id NUMERIC(38,4)   ENCODE az64
	,attribute11 VARCHAR(720)   ENCODE lzo
	,blanket_number NUMERIC(38,4)   ENCODE az64
	,booked_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ooh_context VARCHAR(90)   ENCODE lzo
	,cust_po_number VARCHAR(150)   ENCODE lzo
	,ooh_header_id NUMERIC(38,4)   ENCODE az64
	,order_number NUMERIC(38,4)   ENCODE az64
	,ooh_ordered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,org_id NUMERIC(38,4)   ENCODE az64
	,ooh_sold_to_org_id NUMERIC(38,4)   ENCODE az64
	,actual_shipment_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute8 VARCHAR(720)   ENCODE lzo
	,attribute9 VARCHAR(720)   ENCODE lzo
	,ool_context VARCHAR(90)   ENCODE lzo
	,flow_status_code VARCHAR(90)   ENCODE lzo
	,ool_header_id NUMERIC(38,4)   ENCODE az64
	,ool_inventory_item_id NUMERIC(38,4)   ENCODE az64
	,line_number NUMERIC(38,4)   ENCODE az64
	,line_id NUMERIC(38,4)   ENCODE az64
	,ordered_quantity NUMERIC(38,4)   ENCODE az64
	,ordered_item VARCHAR(6000)   ENCODE lzo
	,unit_selling_price NUMERIC(38,4)   ENCODE az64
	,schedule_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_number NUMERIC(38,4)   ENCODE az64
	,ship_from_org_id NUMERIC(38,4)   ENCODE az64
	,ool_sold_to_org_id NUMERIC(38,4)   ENCODE az64
	,source_document_id NUMERIC(38,4)   ENCODE az64
	,source_document_line_id NUMERIC(38,4)   ENCODE az64
	,name VARCHAR(90)   ENCODE lzo
	,accounting_rule_id BIGINT   ENCODE az64
	,account_class VARCHAR(60)   ENCODE lzo
	,trx_number VARCHAR(60)   ENCODE lzo
	,user_generated_flag VARCHAR(3)   ENCODE lzo
	,party_name VARCHAR(1080)   ENCODE lzo
	,account_number VARCHAR(90)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,territory_short_name VARCHAR(240)   ENCODE lzo
	,attribute1 VARCHAR(720)   ENCODE lzo
	,attribute10 VARCHAR(720)   ENCODE lzo
	,attribute13 VARCHAR(720)   ENCODE lzo
	,attribute14 VARCHAR(720)   ENCODE lzo
	,attribute2 VARCHAR(720)   ENCODE lzo
	,attribute12 VARCHAR(720)   ENCODE lzo
	,attribute7 VARCHAR(720)   ENCODE lzo/*
	,price_by_formula_id NUMERIC(38,4)   ENCODE az64
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cap_scenario_no NUMERIC(38,4)   ENCODE az64*/
)
DISTSTYLE AUTO
;

grant all on table ods_git.nr_shipped_open_order_final_table_stg to group ods_owners;               
--with FINAL_TAB as
delete from ods_git.nr_shipped_open_order_final_table_stg;
insert into ods_git.nr_shipped_open_order_final_table_stg
SELECT distinct 
--mc.segment3,
msib.segment1,
msib.description,
msib.inventory_item_id msib_inventory_item_id,
msib.organization_id,
ooh.attribute11,
ooh.blanket_number,
ooh.booked_date,
ooh.context ooh_context,
ooh.cust_po_number,
ooh.header_id ooh_header_id,
ooh.order_number,
ooh.ordered_date as ooh_ordered_date,
ooh.org_id,
ooh.sold_to_org_id  as ooh_sold_to_org_id,
ool.actual_shipment_date,
ool.attribute8,
ool.attribute9,
ool.context ool_context,
ool.flow_status_code,
ool.header_id ool_header_id,
ool.inventory_item_id ool_inventory_item_id,
ool.line_number,
ool.line_id,
ool.ordered_quantity,
ool.ordered_item,
ool.unit_selling_price,
ool.schedule_ship_date,
ool.shipment_number,
ool.ship_from_org_id,
ool.sold_to_org_id as ool_sold_to_org_id,
ool.source_document_id,
ool.source_document_line_id,
ott.name,
rct.accounting_rule_id,
rct.account_class,
rct.trx_number,
rct.user_generated_flag,
hp.party_name,
hca.account_number,rct.creation_date,ftt.territory_short_name,ooh.attribute1,ooh.attribute10, ooh.attribute13,ooh.attribute14,ool.attribute2,
ooh.attribute12,ool.attribute7--,qpl.price_by_formula_id,qpl.start_date_active,gecap.cap_scenario_no
     FROM
    sc_ods_evncpad1.ont.oe_order_headers_all ooh
	inner join
            sc_ods_evncpad1.ont.oe_order_lines_all ool on
            ooh.header_id = ool.header_id --and ooh.order_number = 63055707
            and ooh.org_id = v_ge_ceo_org
            and ooh.hvr_is_deleted = 0
            and ool.hvr_is_deleted = 0
            inner JOIN  sc_ods_evncpad1.inv.mtl_system_items_b msib
            on (ool.inventory_item_id = msib.inventory_item_id			
			and ool.ship_from_org_id = msib.organization_id)
            and msib.hvr_is_deleted = 0
			inner join sc_ods_evncpad1.ar.hz_cust_accounts hca
            on ooh.sold_to_org_id = hca.cust_account_id
            and hca.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.ar.hz_parties hp
            on hca.party_id = hp.party_id
            and hp.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.ont.oe_transaction_types_tl ott
            on ooh.order_type_id = ott.transaction_type_id
            and ott.hvr_is_deleted = 0
         /*   inner join sc_ods_evncpad1.inv.mtl_item_categories mic
            on (mic.inventory_item_id = ool.inventory_item_id
            and mic.organization_id = msib.organization_id) 
            and mic.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.inv.mtl_category_sets_b mcs
            on mic.category_set_id = mcs.category_set_id
            and mcs.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.inv.mtl_category_sets_tl mcst
            on mcs.category_set_id = mcst.category_set_id
            and mcst.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc
            on mic.category_id = mc.category_id
            and mcs.structure_id = mc.structure_id*/
            inner join sc_ods_evncpad1.applsys.fnd_territories_tl ftt
            on ftt.territory_code = hp.country
            and ftt.hvr_is_deleted = 0
            --inner join sc_ods_evncpad1.cp_mv.hr_operating_units hou
           -- on hou.organization_id = ooh.org_id
         --   inner join sc_ods_evncpad1.ap.ap_invoices_all aia1
          --  on aia1.org_id  = hou.organization_id
         /*   inner join sc_ods_evncpad1.qp.qp_list_headers_b qph
            on qph.list_header_id = ool.price_list_id
            and qph.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.qp.qp_list_lines qpl
            on qph.list_header_id = qpl.list_header_id
            and qpl.hvr_is_deleted = 0
            inner join sc_ods_evncpad1.qp.qp_pricing_attributes qppa
            on qppa.list_line_id = qpl.list_line_id
            AND qppa.list_header_id = qph.list_header_id
            AND qppa.product_attr_value = cast(cast(ool.inventory_item_id as int) as text)
            and qppa.hvr_is_deleted = 0
            left join sc_ods_evncpad1.geaecust.geae_em_esc_cap_line gecap
            on (qppa.list_header_id = gecap.price_list_id
            AND qppa.list_line_id = gecap.price_list_line_id)
            and gecap.hvr_is_deleted = 0*/
            left join
                        (SELECT DISTINCT
                    a.trx_number,a.trx_date,
                    a.creation_date,
                    a.interface_header_attribute1,
                    b.interface_line_attribute6,
                    a.org_id,
                     b.sales_order,--added this
                    b.sales_order_line,--added this
                    c.account_class,  -- Added for US483251
                    nvl(b.accounting_rule_id,0) accounting_rule_id,  -- Added for US483251
                    nvl(c.user_generated_flag,'N') user_generated_flag  -- Added for US483251
                FROM
                    sc_ods_evncpad1.ar.ra_customer_trx_all a,
                    sc_ods_evncpad1.ar.ra_customer_trx_lines_all b,
                    sc_ods_evncpad1.ar.ra_cust_trx_line_gl_dist_all c  -- Added for US483251
                WHERE 1 = 1
				    --AND b.sales_order = '100258'
                    AND a.customer_trx_id = b.customer_trx_id
                    AND b.customer_trx_line_id = c.customer_trx_line_id  -- Added for US483251
                    and a.hvr_is_deleted = 0
                    and b.hvr_is_deleted = 0
                    and c.hvr_is_deleted = 0
                    AND a.org_id = b.org_id  -- Added for US483251
                    AND b.org_id = c.org_id -- Added for US483251
                    AND c.org_id = '101'  -- Added for US483251
                    AND c.account_class = 'REV'  -- Added for US483251
					AND a.batch_source_id = '36182'  -- Added Newly to pull Line Number on 12-Aug-2021
					AND b.customer_trx_id = c.customer_trx_id  -- Added Newly to pull Line Number on 12-Aug-2021
                    AND c.cust_trx_line_gl_dist_id = (
                        SELECT
                            MAX(cust_trx_line_gl_dist_id)
                        FROM
                            sc_ods_evncpad1.ar.ra_cust_trx_line_gl_dist_all
                        WHERE
                            1 = 1
                            AND account_class = 'REV'
                            AND customer_trx_line_id = b.customer_trx_line_id
							AND customer_trx_id = b.customer_trx_id  -- Added Newly to pull Line Number on 12-Aug-2021
							AND org_id = '101'  -- Added Newly to pull Line Number on 12-Aug-2021
                    )  -- Added for US483251
            ) rct on
             trunc(ooh.order_number)::text = rct.interface_header_attribute1             
          and trunc(ool.line_id)::text = rct.interface_line_attribute6
        where 1 = 1        	
          --  AND hou.name = 'GE Commercial Engines Organization'
       /*     AND hou.name IN (SELECT DESCRIPTION 
							FROM sc_ods_evncpad1.applsys.FND_LOOKUP_VALUES
							WHERE 1 = 1
							AND LOOKUP_TYPE = 'GEAE_EM_OPERATING_UNIT'
							AND ENABLED_FLAG = 'Y'
							and hvr_is_deleted = 0
							--AND LANGUAGE = USERENV('LANG')
							AND NVL(END_DATE_ACTIVE,SYSDATE) >= SYSDATE) -- Added for US687220*/
            AND ott.name != 'GENX Progress'
			--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles') -- Added to Test
          --  AND mcst.category_set_name IN ('PRODUCT FINANCE CODE')
		--	AND aia1.source IN ('GEAE_EM_CONC','Manual Invoice Entry')
			AND ool.unit_selling_price * ool.ordered_quantity != 0
			AND ool.cancelled_flag != 'Y'
			AND ooh.attribute11 != 'Spec';
			--AND nvl(ool.schedule_ship_date,SYSDATE) BETWEEN nvl(TO_DATE(gecap.cap_start_date,'YYYY-MM-DD hh24:mi:ss'),nvl(ool.schedule_ship_date,SYSDATE) - 1)	
			--AND nvl(TO_DATE(gecap.cap_end_date,'YYYY-MM-DD hh24:mi:ss'),nvl(ool.schedule_ship_date,SYSDATE) + 1);


CREATE TABLE IF NOT EXISTS ods_git.nr_shipped_open_order_stg
(
	i_msib_inventory_item_id NUMERIC(38,4)   ENCODE az64
	,i_organization_id NUMERIC(38,4)   ENCODE az64
	,i_ool_inventory_item_id NUMERIC(38,4)   ENCODE az64
	,i_ship_from_org_id NUMERIC(38,4)   ENCODE az64
	,i_order_number NUMERIC(38,4)   ENCODE az64
	,i_line_number NUMERIC(38,4)   ENCODE az64
	,i_attribute8 VARCHAR(720)   ENCODE lzo
	,i_name VARCHAR(720)   ENCODE lzo
	,i_ooh_context VARCHAR(90)   ENCODE lzo
	,i_attribute11 VARCHAR(720)   ENCODE lzo
	,i_trx_number VARCHAR(60)   ENCODE lzo
	,i_org_id NUMERIC(38,4)   ENCODE az64
	,i_blanket_number NUMERIC(38,4)   ENCODE az64
	,i_ooh_sold_to_org_id NUMERIC(38,4)   ENCODE az64
	,i_ool_sold_to_org_id NUMERIC(38,4)   ENCODE az64
	,i_segment3 VARCHAR(720)   ENCODE lzo
	,i_ordered_quantity NUMERIC(38,4)   ENCODE az64
	,i_unit_selling_price NUMERIC(38,4)   ENCODE az64
	,i_accounting_rule_id INTEGER   ENCODE az64
	,i_account_class VARCHAR(60)   ENCODE lzo
	,i_user_generated_flag VARCHAR(3)   ENCODE lzo
	,i_ordered_item VARCHAR(6000)   ENCODE lzo
	,i_source_document_id NUMERIC(38,4)   ENCODE az64
	,i_source_document_line_id NUMERIC(38,4)   ENCODE az64
	,i_line_id NUMERIC(38,4)   ENCODE az64
	,i_booked_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,i_schedule_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	--,i_price_by_formula_id NUMERIC(38,4)   ENCODE az64
	,i_party_name VARCHAR(1080)   ENCODE lzo
	,i_account_number VARCHAR(90)   ENCODE lzo
	,i_ool_header_id NUMERIC(38,4)   ENCODE az64
	,i_ooh_header_id NUMERIC(38,4)   ENCODE az64
	,i_shipment_number NUMERIC(38,4)   ENCODE az64
	,i_actual_shipment_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,i_flow_status_code VARCHAR(90)   ENCODE lzo
	,i_ooh_ordered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,i_attribute9 VARCHAR(720)   ENCODE lzo
	,i_ool_context VARCHAR(90)   ENCODE lzo
	,rsp_concessions1 VARCHAR(720)   ENCODE lzo
	,rsp_concessions2 VARCHAR(720)   ENCODE lzo
	,rsp_concessions3 VARCHAR(720)   ENCODE lzo
	,vfnd_lookup_values INTEGER   ENCODE az64
	,vsegment VARCHAR(120)   ENCODE lzo
	,vsegment3 VARCHAR(120)   ENCODE lzo
	,vunit_cost1 VARCHAR(720)   ENCODE lzo
	,vunit_cost2 VARCHAR(720)   ENCODE lzo
	,vcase2basetran NUMERIC(38,4)   ENCODE az64
	,vlineno INTEGER   ENCODE az64
	,vunit_cost3 VARCHAR(720)   ENCODE lzo
	,vunit_cost4 VARCHAR(720)   ENCODE lzo
	,vunit_cost5 VARCHAR(720)   ENCODE lzo
	,department VARCHAR(720)   ENCODE lzo
	,engine_family VARCHAR(256)   ENCODE lzo
	,engine_model VARCHAR(256)   ENCODE lzo
	,order_type VARCHAR(256)   ENCODE lzo
	,sub_family VARCHAR(256)   ENCODE lzo
	,invoice_number VARCHAR(256)   ENCODE lzo
	,contract_name VARCHAR(256)   ENCODE lzo
	,ultimate_customer VARCHAR(256)   ENCODE lzo
	,ultimate_customer_number VARCHAR(256)   ENCODE lzo
	,install_spare VARCHAR(256)   ENCODE lzo
	,product_type VARCHAR(256)   ENCODE lzo
	,ordered_quantity NUMERIC(38,4)   ENCODE az64
	,unit_selling_price NUMERIC(38,4)   ENCODE az64
	,sales_old NUMERIC(38,4)   ENCODE az64
	,sales NUMERIC(38,4)   ENCODE az64
	,concessions_old NUMERIC(38,4)   ENCODE az64
	,concessions NUMERIC(38,4)   ENCODE az64
	,concessions_discount_old NUMERIC(38,4)   ENCODE az64
	,concessions_discount NUMERIC(38,4)   ENCODE az64
	,net_sales NUMERIC(38,4)   ENCODE az64
	,net_sales_old NUMERIC(38,4)   ENCODE az64
	,order_number NUMERIC(38,4)   ENCODE az64
	,line_number NUMERIC(38,4)   ENCODE az64
	,cust_po_number VARCHAR(256)   ENCODE lzo
	,item_code VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,concessions_ap_misc NUMERIC(38,4)   ENCODE az64
	,shop_cost VARCHAR(720)   ENCODE lzo
	,rsp_sales VARCHAR(720)   ENCODE lzo
	,rsp_concessions VARCHAR(720)   ENCODE lzo
	,rsp_concessionf VARCHAR(720)   ENCODE lzo
	,rsp_concession_actual_ap VARCHAR(720)   ENCODE lzo
	,rsp_concession_actual_misc_ar VARCHAR(720)   ENCODE lzo
	,ordered_date DATE   ENCODE az64
	,promise_date DATE   ENCODE az64
	,other_cost VARCHAR(720)   ENCODE lzo
	,ship_date DATE   ENCODE az64
	,ship_year INTEGER   ENCODE az64
	,ship_quarter VARCHAR(99)   ENCODE lzo
	,ship_month VARCHAR(99)   ENCODE lzo
	,invoice_date DATE   ENCODE az64
	,ordered_month VARCHAR(720)   ENCODE lzo
	,ordered_year INTEGER   ENCODE az64
	,ordered_quarter VARCHAR(99)   ENCODE lzo
	,country VARCHAR(720)   ENCODE lzo
	,serial_number VARCHAR(720)   ENCODE lzo
	,line_type VARCHAR(720)   ENCODE lzo
	,reverser_nacells VARCHAR(720)   ENCODE lzo
	,customer_ref_number VARCHAR(720)   ENCODE lzo
	,aircraft_msn VARCHAR(720)   ENCODE lzo
	,aircraft_number VARCHAR(720)   ENCODE lzo
	,aircraft_delivery_date DATE   ENCODE az64
	,po_ship_date DATE   ENCODE az64
	,contract_price VARCHAR(720)   ENCODE lzo
	,escalation_code VARCHAR(720)   ENCODE lzo
	,index1_base VARCHAR(720)   ENCODE lzo
	,index2_base VARCHAR(720)   ENCODE lzo
	,base_year VARCHAR(720)   ENCODE lzo
	,escalation_cap_code VARCHAR(720)   ENCODE lzo
	,unit_price NUMERIC(38,4)   ENCODE az64
	,gta_letter VARCHAR(720)   ENCODE lzo
	,order_trx_name VARCHAR(720)   ENCODE lzo
	,ordered_item VARCHAR(720)   ENCODE lzo
	,attribute8 VARCHAR(720)   ENCODE lzo
	,record_type VARCHAR(720)   ENCODE lzo
	,oar_customer_number VARCHAR(30)   ENCODE lzo
	,credit_cd VARCHAR(30)   ENCODE lzo
	,qty NUMERIC(38,4)

)
DISTSTYLE AUTO
;

grant all on table ods_git.nr_shipped_open_order_stg to group ods_owners;
delete ods_git.nr_shipped_open_order_stg;                     
insert into ods_git.nr_shipped_open_order_stg
( i_msib_inventory_item_id,
i_organization_id,
i_ool_inventory_item_id,
i_ship_from_org_id,
i_order_number,
i_line_number,
i_attribute8,
i_name,
i_ooh_context,
i_attribute11,
i_trx_number,
i_org_id,
i_blanket_number,
i_ooh_sold_to_org_id,
i_ool_sold_to_org_id,
--i_segment3,
i_ordered_quantity,
i_unit_selling_price,
i_accounting_rule_id,
i_account_class,
i_user_generated_flag,
i_ordered_item,
i_source_document_id,
i_source_document_line_id,
i_line_id,
i_booked_date,
i_schedule_ship_date,
--i_price_by_formula_id,
i_party_name,i_account_number,i_ool_header_id,i_ooh_header_id,i_shipment_number,i_actual_shipment_date,i_flow_status_code,i_ooh_ordered_date,
order_number,line_number,cust_po_number,item_code,description,invoice_date,ordered_year,ordered_quarter,ordered_month,
country,ordered_date,promise_date,serial_number,line_type,customer_ref_number,Aircraft_MSN,Aircraft_Number,
aircraft_delivery_date,po_ship_date,contract_price,order_trx_name,ordered_item,--base_year,--ship_year,ship_quarter,ship_month,
--escalation_cap_code,
unit_price,attribute8,i_attribute9,i_ool_context
)
         select msib_inventory_item_id,
organization_id,
ool_inventory_item_id,
ship_from_org_id,
trunc(order_number)::text,
trunc(line_number)::text,
attribute8,
name,
ooh_context,
attribute11,
trx_number,
org_id,
blanket_number,
ooh_sold_to_org_id,
ool_sold_to_org_id,
--segment3,
ordered_quantity,
unit_selling_price::int,
accounting_rule_id,
account_class,
user_generated_flag,
ordered_item,
source_document_id,
source_document_line_id,
line_id,
booked_date,
schedule_ship_date,
--price_by_formula_id,
party_name,account_number,ool_header_id,ooh_header_id,shipment_number,actual_shipment_date,flow_status_code,ooh_ordered_date,
order_number,
line_number,
cust_po_number,
segment1,
description,
creation_date,
nvl(TO_CHAR(ooh_ordered_date,'YYYY'),'0')::int,
nvl2(TO_CHAR(ooh_ordered_date,'Q'),TO_CHAR(ooh_ordered_date,'Q'),'NA'),
nvl(TO_CHAR(ooh_ordered_date,'MON'),'NA'),
nvl(territory_short_name,'NA'),
TO_CHAR(ooh_ordered_date,'DD-MON-YYYY')::date,
TO_CHAR(schedule_ship_date,'DD-MON-YYYY')::date,
nvl(attribute1,'NA'),
nvl(attribute8,'NA'),
nvl(attribute10,'NA'),
nvl(attribute13,'NA'),
nvl(attribute14,'NA'),
(attribute2)::date,
(to_char(to_date(attribute12,'YYYY-MM-DD HH24:MI:SS'),'DD-MON-YYYY'))::date,
nvl(attribute7,'0'),
nvl(name,'NA'),
nvl(ordered_item,'NA'),
--(to_char(start_date_active,'DD-MON-YYYY')),
--nvl(to_char(ship_date,'YYYY'),'0')::int,
--nvl(to_char(ship_date,'Q'),'0'),
--nvl(to_char(ship_date,'MON-YY'),'0'),
--((cap_scenario_no::text)),
nvl(unit_selling_price,0),
nvl(attribute8,'NA'), attribute9,ool_context
from ods_git.nr_shipped_open_order_final_table_stg;    

vstatus := 'Insert complete';


 


   update ods_git.nr_shipped_open_order_stg i
      set record_type=    CASE
                        WHEN i.i_flow_status_code = 'CLOSED'
                             OR i.i_flow_status_code = 'SHIPPED' THEN 'SHIPPED'
                        ELSE 'BACKLOG'
                    end;
                   ----vstatus := 'Step 1 complete';
                   
  update ods_git.nr_shipped_open_order_stg i
               set department =
                (select 
                    substring(mc.segment2,0,position('-' in mc.segment2))
                FROM
                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                    sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                    sc_ods_evncpad1.inv.mtl_system_items_b msib
                WHERE
					mic.hvr_is_deleted = 0 
					and mcst.hvr_is_deleted = 0
                    and msib.inventory_item_id = mic.inventory_item_id
                    AND msib.organization_id = mic.organization_id
                    AND msib.inventory_item_id = i.i_ool_inventory_item_id
                    AND msib.organization_id = i.i_ship_from_org_id
                    AND mic.inventory_item_id = msib.inventory_item_id
                    AND mic.organization_id = msib.organization_id
                    AND mic.category_set_id = mcs.category_set_id
                    AND mcs.category_set_id = mcst.category_set_id
                    AND mcst.category_set_name = 'PRODUCT LINE (FAA TYPE)'
                    AND mic.category_id = mc.category_id)--orgid_tmp -- Added to Test
             ;      
vstatus := 'Update to department complete';
    update ods_git.nr_shipped_open_order_stg i
               set engine_family =
                (SELECT
                    substring(mc.segment2,0,position('-' in mc.segment2))
                FROM
                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                    sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                    sc_ods_evncpad1.inv.mtl_system_items_b msib
                WHERE
                    mic.hvr_is_deleted = 0 
					and mcst.hvr_is_deleted = 0
                    and msib.inventory_item_id = mic.inventory_item_id
                    AND msib.organization_id = mic.organization_id
                    AND msib.inventory_item_id = i.i_ool_inventory_item_id
                    AND msib.organization_id = i.i_ship_from_org_id
                    AND mic.inventory_item_id = msib.inventory_item_id
                    AND mic.organization_id = msib.organization_id
                    AND mic.category_set_id = mcs.category_set_id
                    AND mcs.category_set_id = mcst.category_set_id
                    AND mcst.category_set_name = 'PRODUCT LINE (FAA TYPE)'
                    AND mic.category_id = mc.category_id)--orgid_tmp -- Added to Test
             ;  
            
            vstatus := 'Updating Segment2 complete';
                  update ods_git.nr_shipped_open_order_stg i
               set engine_model =
                (SELECT
                    mc.segment2
                FROM
                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                    sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                    sc_ods_evncpad1.inv.mtl_system_items_b msib
                WHERE
                    mic.hvr_is_deleted = 0 
					and mcst.hvr_is_deleted = 0
                    and msib.inventory_item_id = mic.inventory_item_id
                    AND msib.organization_id = mic.organization_id
                    AND msib.inventory_item_id = i.i_ool_inventory_item_id
                    AND msib.organization_id = i.i_ship_from_org_id
                    AND mic.inventory_item_id = msib.inventory_item_id
                    AND mic.organization_id = msib.organization_id
                    AND mic.category_set_id = mcs.category_set_id
                    AND mcs.category_set_id = mcst.category_set_id
                    AND mcst.category_set_name = 'PRODUCT LINE (FAA TYPE)'
                    AND mic.category_id = mc.category_id)--orgid_tmp -- Added to Test
             ;
            vstatus := 'Updating engine_model complete';
                  update ods_git.nr_shipped_open_order_stg i
               set sub_family =
                (SELECT
                    mc.segment2
                FROM
                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                    sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                    sc_ods_evncpad1.inv.mtl_system_items_b msib,orgid_tmp v
                WHERE
                     mic.hvr_is_deleted = 0 
					and mcst.hvr_is_deleted = 0
                    and msib.inventory_item_id = mic.inventory_item_id
                    AND msib.organization_id = mic.organization_id
                    AND msib.inventory_item_id = i.i_ool_inventory_item_id
                    AND msib.organization_id = i.i_ship_from_org_id
                    AND mic.inventory_item_id = msib.inventory_item_id
                    AND mic.organization_id = msib.organization_id
                    AND mic.category_set_id = mcs.category_set_id
                    AND mcs.category_set_id = mcst.category_set_id
                    AND mcst.category_set_name = 'PRODUCT LINE (FAA TYPE)'
                    AND mic.category_id = mc.category_id
					AND mic.organization_id = v.organization_id)--orgid_tmp -- Added to Test
             ;

            vstatus := 'Updating subfamily complete';
            update ods_git.nr_shipped_open_order_stg i
					set vsegment =
                         (SELECT
                            mc.segment3 
                        FROM
                            sc_ods_evncpad1.inv.mtl_item_categories mic,
                            sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                            sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                            sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                            orgid_tmp v
                        WHERE
                            mic.hvr_is_deleted = 0 
							and mcst.hvr_is_deleted = 0
							and mcst.category_set_name = 'PRODUCT FINANCE CODE'
                            AND mic.inventory_item_id = i.i_msib_inventory_item_id
                            AND mic.organization_id = i.i_organization_id
                            AND mic.category_set_id = mcs.category_set_id
                            AND mcs.category_set_id = mcst.category_set_id
                            AND mc.category_id = mic.category_id
                            AND mcs.structure_id = mc.structure_id
							AND mic.organization_id = v.organization_id)-- orgid_tmp -- Added to Test
                    ;
                 
			vstatus := 'Step vsegment complete';
                   update ods_git.nr_shipped_open_order_stg i
                   set vfnd_lookup_values =
                       (SELECT
                            count(1)    -- Added Condition for US483251 Starts
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type = 'GEAE_CEO_ORDER_TYPE'
                            AND "tag" = 'RETURN'
                            AND enabled_flag = 'Y'
							AND hvr_is_deleted = 0
                            AND i.i_name = meaning);
vstatus := 'Step 7 complete';
 update ods_git.nr_shipped_open_order_stg 
   set order_type =     
                CASE
                    WHEN i_ooh_context = 'CEO'
                         AND i_attribute11 = 'Firm' THEN 'SO'
                    WHEN i_attribute11 = 'Planned' THEN 'SP'
					WHEN i_attribute11 = 'Spec' THEN 'SS'
                    ELSE NULL
                END
          ;
vstatus := 'Step 8 complete';
 update ods_git.nr_shipped_open_order_stg i
 set invoice_number =
			 (nvl(i.i_trx_number,( SELECT DISTINCT
										a.trx_number
									FROM
										sc_ods_evncpad1.ar.ra_customer_trx_all a,sc_ods_evncpad1.ar.ra_customer_trx_lines_all b
									WHERE
										a.customer_trx_id = b.customer_trx_id
										AND a.interface_header_attribute1 = i.i_order_number--Changed to_char to text
										AND b.sales_order_line = '1'
										AND a.hvr_is_deleted = 0 and b.hvr_is_deleted = 0
										AND a.org_id = i.i_org_id
								) ))  ;
								
		vstatus := 'Step 9 complete';
	update ods_git.nr_shipped_open_order_stg i							
      set contract_name =      (select distinct
                    sales_document_name
                FROM
                    sc_ods_evncpad1.ont.oe_blanket_headers_all
                WHERE
                    order_number = i.i_blanket_number
                    AND sold_to_org_id = i.i_ooh_sold_to_org_id
                      and hvr_is_deleted = 0);
                 
                 
                 
         
	 vstatus := 'Step 10 complete';                       
    update ods_git.nr_shipped_open_order_stg i		               
     set ultimate_customer =    
                CASE
                    WHEN ( i.vsegment = 'CD' ) THEN (
                        SELECT
                            a.party_name
                        FROM
                            sc_ods_evncpad1.ar.hz_parties a,
                            sc_ods_evncpad1.ar.hz_cust_accounts b
                        WHERE
                            a.party_id = b.party_id
                            AND b.cust_account_id = i.i_ool_sold_to_org_id
							AND a.hvr_is_deleted = 0 and b.hvr_is_deleted = 0
                    )
                    ELSE i.i_party_name
                end;
         
         vstatus := 'Step 11 complete';  
        update ods_git.nr_shipped_open_order_stg i		               
     set ultimate_customer_number =
                CASE
                    WHEN ( i.vsegment = 'CD' ) THEN (
                        SELECT
                            b.account_number
                        FROM
                            sc_ods_evncpad1.ar.hz_parties a,
                            sc_ods_evncpad1.ar.hz_cust_accounts b
                        WHERE
                            a.party_id = b.party_id
                            AND b.cust_account_id = i.i_ool_sold_to_org_id
							AND a.hvr_is_deleted = 0 and b.hvr_is_deleted = 0
                    )
                    ELSE i.i_account_number
                end;
         vstatus := 'Step 12 complete';
        
          update ods_git.nr_shipped_open_order_stg i		               
     set oar_customer_number =
                          case	
                    WHEN ( i.vsegment = 'CD' ) THEN (
                        SELECT
                            account_number
                        FROM
                            sc_ods_evncpad1.ar.hz_cust_accounts
                        WHERE
                            cust_account_id = i.i_ool_sold_to_org_id
							AND hvr_is_deleted = 0 
                    )
                    ELSE i.i_account_number
                END
             ;
            
            
               
           
                    drop table if exists nr_order_quantity_tmp;
        create temp table nr_order_quantity_tmp distkey (i_tmp_inventory_item_id) as
        (
            SELECT
                distinct i_ool_inventory_item_id i_tmp_inventory_item_id, 
                i_ship_from_org_id i_tmp_ship_from_org_id, 
                mc.segment3,
                i.i_name i_tmp_name
            FROM
                sc_ods_evncpad1.inv.mtl_item_categories mic,
                sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                sc_ods_evncpad1.inv.mtl_category_sets_b mcs,
                sc_ods_evncpad1.inv.mtl_categories_b mc,
                sc_ods_evncpad1.inv.mtl_system_items_b msib,
                ods_git.nr_shipped_open_order_stg i
            WHERE
                msib.inventory_item_id = mic.inventory_item_id
                AND msib.organization_id = mic.organization_id
                AND msib.inventory_item_id = i_ool_inventory_item_id
                AND msib.organization_id = i.i_ship_from_org_id
                AND mic.inventory_item_id = msib.inventory_item_id
                AND mic.organization_id = msib.organization_id
                AND mic.category_set_id = mcs.category_set_id
                AND mcs.category_set_id = mcst.category_set_id
                AND mcst.category_set_name = 'PRODUCT FINANCE CODE'
                --AND mcst.language = userenv('LANG')
                AND mic.category_id = mc.category_id
                and mic.hvr_is_deleted = 0
                and mcst.hvr_is_deleted = 0
                and mcs.hvr_is_deleted = 0
                and mc.hvr_is_deleted = 0
                and msib.hvr_is_deleted = 0 
        );
      
          
              update ods_git.nr_shipped_open_order_stg i		      
    set  install_spare  =     (
                SELECT
                    qp_rn.installs_spares_others
                FROM
                    sc_ods_evncpad1.cp_mv.q_geae_ceo_om_install_revers qp_rn
                WHERE
                    qp_rn.om_order_type = i.i_name
                    AND qp_rn.product_finance_code IS NOT NULL
                    AND qp_rn.product_finance_code = i.i_segment3
            );
           
   
            vstatus := 'Step 13 complete';

   update ods_git.nr_shipped_open_order_stg i		      
    set  install_spare =        
            (
                select distinct
                    qp_rn1.installs_spares_others
                FROM
                    sc_ods_evncpad1.cp_mv.q_geae_ceo_om_install_revers qp_rn1
                WHERE
                    qp_rn1.product_finance_code IS NULL
                    AND qp_rn1.om_order_type = i.i_name
            )
     where install_spare is null;
            
         vstatus := 'Step 14 complete';
	    update ods_git.nr_shipped_open_order_stg i
			set product_type = (CASE WHEN i.i_attribute8 ='GE Manufactured Item' and  upper(i.i_name) in -- Added for DE77456 Start
				 (select upper(meaning) 
				   FROM 
                    sc_ods_evncpad1.applsys.fnd_lookup_values flv
				   WHERE 1 = 1
				   AND lookup_type = 'GEAE_CEO_HYPERION_PRD_TYPE' 
				   AND enabled_flag = 'Y'
				   AND hvr_is_deleted = 0
				   AND TRUNC(SYSDATE) BETWEEN START_DATE_ACTIVE AND NVL(END_DATE_ACTIVE,TO_DATE('31-DEC-4712','DD-MON-YYYY'))
				   --AND language = userenv('LANG')
                   and flv.hvr_is_deleted = 0
				   )
				THEN '10'                                                                
			ELSE   -- Added for DE77456 End
			(SELECT
                    oqtmp.segment3
                from nr_order_quantity_tmp oqtmp
                where i.i_ool_inventory_item_id = oqtmp.i_tmp_inventory_item_id
                    and i.i_ship_from_org_id = oqtmp.i_tmp_ship_from_org_id
                    and i.i_name = oqtmp.i_tmp_name
            ) END ); 
           
          vstatus := 'Step 15 complete';
       update ods_git.nr_shipped_open_order_stg i
                    set qty = 0;

        update ods_git.nr_shipped_open_order_stg i 
                set qty = i.i_ordered_quantity
                from nr_order_quantity_tmp oqtmp
                where i.i_ool_inventory_item_id = oqtmp.i_tmp_inventory_item_id
                    and i.i_ship_from_org_id = oqtmp.i_tmp_ship_from_org_id
                    and oqtmp.segment3 IN ('A0', '10', '1K', '1F', 'AF', 'AK')
                    and i.i_attribute8 IN (
                                        SELECT
                                            meaning
                                        FROM
                                            sc_ods_evncpad1.applsys.fnd_lookup_values flv
                                        WHERE
                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                            AND enabled_flag = 'Y'
                                            and flv.hvr_is_deleted = 0
                                            --AND language = userenv('LANG')
                                        );

                                       
                                  
    
vstatus := 'Step 16 complete';
 update ods_git.nr_shipped_open_order_stg i	
     set unit_selling_price =          
                CASE
                    WHEN vfnd_lookup_values > 0 THEN (-1 ) * i.i_unit_selling_price::int * i.i_ordered_quantity::int
                    ELSE i.i_unit_selling_price * i.i_ordered_quantity
                END
            ;
           vstatus := 'Step 17 complete';
        update ods_git.nr_shipped_open_order_stg i	            
                   set sales =    
               case when i.i_attribute8 IN (
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                            AND enabled_flag = 'Y'
							AND hvr_is_deleted = 0
                    ) 
               THEN 
                   case when vfnd_lookup_values > 0 THEN  ( (-1 ) * ( i.i_unit_selling_price * i.i_ordered_quantity ) )
                        when 
                                nvl(i.i_accounting_rule_id,0) = '2000'
                                AND upper(i.i_account_class) = 'REV'
                                AND i.i_user_generated_flag = 'Y'
                                AND (
                                    upper(i.i_ordered_item) LIKE '%BREE'
                                    OR upper(i.i_ordered_item) LIKE '%BRAE'
                                    OR upper(i.i_ordered_item) LIKE '%BPAE'
                                    OR upper(i.i_ordered_item) LIKE '%BRPE'
                                )
                                 OR ( nvl(i.i_accounting_rule_id,0) != '2000' ) 
                                  THEN ( i.i_unit_selling_price * i.i_ordered_quantity )
                            ELSE 0
                        end
                    ELSE  0
               end; 
                vstatus := 'Step 18 complete';          
    update ods_git.nr_shipped_open_order_stg i	            
       set concessions =
            (
                CASE i.attribute8
                    WHEN (
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type IN (
                                'GEAE_CEO_CONCESSION_ITEM_TYPE',
                                'GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                            )
                            AND enabled_flag = 'Y'  -- Added on 05-Jul-2021 for US483251
							AND hvr_is_deleted = 0
                            AND meaning = i.i_attribute8
                    ) THEN ( i.i_unit_selling_price * i.i_ordered_quantity )
                    ELSE 0
                END
            );	  

            vstatus := 'Step 19 complete';
        update ods_git.nr_shipped_open_order_stg i	            
        set concessions_discount = 
            case when i.attribute8 IN (
                SELECT
                    meaning
                FROM
                    sc_ods_evncpad1.applsys.fnd_lookup_values
                WHERE
                    lookup_type = 'GEAE_CEO_DISCOUNT_ITEM_TYPE'
                    AND enabled_flag = 'Y'
					AND hvr_is_deleted = 0
                    --AND language = userenv('LANG')  --Prashanth commented
            ) THEN case WHEN vfnd_lookup_values > 0 THEN ( (-1 ) * i.unit_selling_price * i.ordered_quantity )
                    ELSE i.unit_selling_price * i.ordered_quantity
                end            
            ELSE 0
        end;
vstatus := 'Step 20 complete';
    update ods_git.nr_shipped_open_order_stg i	            
        set  net_sales =            
            case when vfnd_lookup_values > 0 then 
                    i.i_unit_selling_price * i.i_ordered_quantity * (-1 )
                ELSE 
                    case when (
                            nvl(i.i_accounting_rule_id,0) = '2000'
                            AND upper(i.i_account_class) = 'REV'
                            AND i.i_user_generated_flag = 'Y'-- Added for US483251 to pull price as 0 when Accounting Rule is GE_DEFERRED_REV AND 11C Cost will Show in Shop Cost 
                            AND (
                                upper(i.i_ordered_item) LIKE '%BREE'
                                OR upper(i.i_ordered_item) LIKE '%BRAE'
                                OR upper(i.i_ordered_item) LIKE '%BPAE'
                                OR upper(i.i_ordered_item) LIKE '%BRPE'
                            )
                        )
                                OR ( nvl(i.i_accounting_rule_id,0) != '2000' ) THEN  -- Added for US483251 to pull price as 0 when Accounting Rule is GE_DEFERRED_REV AND 11C Cost will Show in Shop Cost
                            --   net_sales_old := i.unit_selling_price * i.ordered_quantity + nvl( vamount,0);
                                ( i.i_unit_selling_price * i.i_ordered_quantity )
                            
                        ELSE
                        0 
                    end
                    
            end;

            
vstatus := 'Step 21 complete';

 update ods_git.nr_shipped_open_order_stg i
 set vsegment3 =	   (
                        SELECT
                            ( SUM(calc.calc_rsp_share_amt) * (-1 ) )
                        FROM
                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                            sc_ods_evncpad1.ont.oe_order_headers_all oh,
                            sc_ods_evncpad1.ont.oe_order_lines_all ol
                        WHERE
                            1 = 1
                            AND i.i_ooh_header_id = i.i_ool_header_id
                            AND calc.trx_source_line_id = ol.line_id
                            AND calc.trx_source_id = oh.header_id
                            AND oh.header_id = i.i_source_document_id
                            AND ol.line_id = i.i_source_document_line_id
                            AND oh.header_id = ol.header_id
                            AND calc.calc_status = 'VALID'
                            AND calc.calc_type = 'FORECAST'
                            AND calc.trx_source = 'OM'
							AND calc.hvr_is_deleted = 0
							AND oh.hvr_is_deleted = 0
							AND ol.hvr_is_deleted = 0
                            AND i.i_attribute8 IN (
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type IN (
                                        'GEAE_CEO_CONCESSION_ITEM_TYPE',
                                        'GEAE_CEO_DISCOUNT_ITEM_TYPE',
                                        'GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                                    )
                                    AND enabled_flag = 'Y'
									AND hvr_is_deleted = 0
                                    --AND language = userenv('LANG')
                                                                )
                                                                GROUP BY
                            i.i_order_number,
                            i.i_line_number
                    ) ;    
                vstatus := 'Step 22 complete';   
       update ods_git.nr_shipped_open_order_stg i
 set rsp_concessionf =             
 CASE
                    WHEN vfnd_lookup_values > '0' THEN vsegment3
                    ELSE nvl( (
                        SELECT
                            SUM(calc.calc_rsp_share_amt)
                        FROM
                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc
                        WHERE
                            1 = 1
                            AND i.i_ooh_header_id = i.i_ool_header_id
                            AND calc.trx_source_line_id = i.i_line_id
                            AND calc.trx_source_id = i.i_ooh_header_id
                            AND calc.calc_status = 'VALID'
                            AND calc.calc_type = 'FORECAST'
                            AND calc.trx_source = 'OM'
							AND calc.hvr_is_deleted = 0
							--  AND ooh.org_id = 121179
                            AND i.i_attribute8 IN(
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type IN(
                                        'GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_DISCOUNT_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                                    )
                                    AND enabled_flag = 'Y'
									AND hvr_is_deleted = 0
                                    --AND language = userenv('LANG')
                            )
                        GROUP BY
                            i.i_order_number,i.i_line_number
                    ),0)::text
                END
            ;
vstatus := 'Step 23 complete';
	update ods_git.nr_shipped_open_order_stg i
			set rsp_concession_actual_ap = (SELECT
                    SUM(calc.calc_rsp_share_amt)
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc
                    inner join sc_ods_evncpad1.ap.ap_invoice_lines_all aila
                    	on aila.invoice_id = calc.trx_source_id
                    		AND aila.line_number = calc.trx_source_line_id
		                    AND calc.so_number = aila.attribute1
        		            AND calc.so_line_number = aila.attribute2
					inner join sc_ods_evncpad1.applsys.fnd_lookup_values flv
						on i.i_attribute8 = flv.meaning 
							and flv.lookup_type IN (
                                'GEAE_CEO_CONCESSION_ITEM_TYPE',
                                'GEAE_CEO_DISCOUNT_ITEM_TYPE',
                                'GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                            )
                            AND flv.enabled_flag = 'Y'
                            --AND language = userenv('LANG')
                            and flv.hvr_is_deleted = 0
                where 1=1
                    and aila.attribute1 = trunc(i.i_order_number)--'100270'
                    AND aila.attribute2 = trunc(i.i_line_number)-- '3'
                    AND aila.org_id = i.i_org_id
                    AND calc.calc_status = 'VALID'
                    AND calc.calc_type = 'ACTUAL'
                    AND calc.trx_source = 'AP'
                    and calc.hvr_is_deleted = 0
                    and aila.hvr_is_deleted = 0
            );  
     
vstatus := 'Step 24 complete';
		update ods_git.nr_shipped_open_order_stg i
			set rsp_concession_actual_misc_ar = (SELECT
                    SUM(calc.calc_rsp_share_amt)
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                    sc_ods_evncpad1.ar.ra_customer_trx_lines_all rctl
                WHERE
                    1 = 1
                    AND calc.trx_source_id = rctl.customer_trx_id
                    AND calc.trx_source_line_id = rctl.customer_trx_line_id
                    AND rctl.sales_order = i.i_order_number  --'100271'
                    AND rctl.sales_order_line = trunc(i.i_line_number)
                                                            || '.'
                                                            || trunc(i.i_shipment_number) --'3.1'
                    AND calc.calc_status = 'VALID'
                    AND calc.calc_type = 'ACTUAL'
                    AND calc.trx_source = 'Misc AR'
                    AND i.i_attribute8 IN (
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values flv
                        WHERE
                            lookup_type IN (
                                'GEAE_CEO_CONCESSION_ITEM_TYPE',
                                'GEAE_CEO_DISCOUNT_ITEM_TYPE',
                                'GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                            )
                            AND enabled_flag = 'Y'
                            and flv.hvr_is_deleted = 0
                            --AND language = userenv('LANG')
                    )
                    and calc.hvr_is_deleted = 0
                    and rctl.hvr_is_deleted = 0
            );
vstatus := 'Step 25 complete';
update ods_git.nr_shipped_open_order_stg i
					set vsegment3 =
                          (SELECT
                                mc.segment3
                            FROM
                                sc_ods_evncpad1.inv.mtl_item_categories mic,
                                sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                                sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                                sc_ods_evncpad1.inv.mtl_system_items_b msb,orgid_tmp v                                
                            where
                                mcst.category_set_name = 'PRODUCT FINANCE CODE'
                                AND msb.inventory_item_id = i.i_ool_inventory_item_id
                                AND msb.organization_id = i.i_ship_from_org_id
                                AND mic.inventory_item_id = i.i_msib_inventory_item_id
                                AND mic.organization_id = i.i_organization_id                                
                             -- and msb.inventory_item_id = mic.inventory_item_id
                             -- AND i.i_ship_from_org_id = mic.organization_id
                                AND mic.category_set_id = mcst.category_set_id                                 
                                AND mic.category_id = mc.category_id
								AND mic.organization_id =v.organization_id
								AND mic.hvr_is_deleted = 0
								AND mcst.hvr_is_deleted = 0
								AND msb.hvr_is_deleted = 0)--vorgid-- Added to Test
                        ;
  vstatus := 'Step 26 complete';                     
    update ods_git.nr_shipped_open_order_stg i
         set other_cost =        (
                CASE
                    WHEN (
                        ( vsegment3 IN (
                            '1F',
                            'AF'
                        ) )
                        AND i.i_actual_shipment_date IS NULL
                    ) THEN (
                           
                        SELECT
                            character5
                        FROM
                            sc_ods_evncpad1.inv.mtl_item_categories mic,
                            sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                            sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                            sc_ods_evncpad1.qa.qa_results qr,
                            sc_ods_evncpad1.qa.qa_plans qp
                        WHERE
                            mcst.category_set_name = 'CEO_ENGINE_COST'
                            AND mic.category_set_id = mcst.category_set_id
                            AND mc.category_id = mic.category_id
                            AND mic.organization_id = i.i_ship_from_org_id
                            AND mic.inventory_item_id = i.i_ool_inventory_item_id
                            --AND mcst.language = userenv('LANG')
                            AND mc.segment1 = qr.character1
                            AND qr.plan_id = qp.plan_id
                            AND qr.organization_id = i.i_ship_from_org_id
                            AND qp.name = 'CEO_FUTURE_SHOP_COST'
							AND mic.hvr_is_deleted = 0
							AND mcst.hvr_is_deleted = 0
							AND qr.hvr_is_deleted = 0
							AND qp.hvr_is_deleted = 0
                            AND i.i_schedule_ship_date BETWEEN TO_DATE(character2,'YYYY-MM-DD hh24:mi:ss') AND nvl(TO_DATE(character3,'YYYY-MM-DD hh24:mi:ss'),'31-DEC-2471'
             ) )
                    WHEN (
                        ( vsegment3 IN (
                            '1F',
                            'AF'
                        ) )
                        AND i.i_actual_shipment_date IS NOT NULL
                    ) THEN '0' 
                    ELSE '0'
                END
            );
           vstatus := 'Step 27 complete';

        update ods_git.nr_shipped_open_order_stg i
        set ship_date =
            CASE
                WHEN i.i_flow_status_code = 'CLOSED' OR i.i_flow_status_code = 'SHIPPED' 
                THEN
                    CASE
                        WHEN i.vfnd_lookup_values > 0 THEN
                            (SELECT
                                i.i_ooh_ordered_date
                            FROM
                                sc_ods_evncpad1.ont.oe_order_headers_all oh,
                                sc_ods_evncpad1.ont.oe_order_lines_all ol
                            WHERE
                                oh.header_id = ol.header_id
                                AND oh.header_id = i.i_source_document_id
                                AND ol.line_id = i.i_source_document_line_id
                                and oh.hvr_is_deleted = 0
                                and ol.hvr_is_deleted = 0)
                        end                                
                    end;
           vstatus := 'Step 28 complete';                                
              update ods_git.nr_shipped_open_order_stg i
          set ship_date =
                        CASE
                            WHEN i.i_flow_status_code = 'CLOSED' OR i.i_flow_status_code = 'SHIPPED' THEN
                                CASE
                                    WHEN vfnd_lookup_values <= 0 THEN                                  
                                    
                                      (
                                        SELECT
                                            a.trx_date
                                        FROM
                                            sc_ods_evncpad1.ar.ra_customer_trx_all a,
                                            sc_ods_evncpad1.ar.ra_customer_trx_lines_all b
                                        WHERE
                                            a.customer_trx_id = b.customer_trx_id
                                            AND a.interface_header_attribute1 = trunc(i.i_order_number)::text
                                            AND a.org_id = i.i_org_id--ool.ship_from_org_id
                                            AND b.interface_line_attribute6 = trunc(i.i_line_id)::text
                                            AND a.org_id = i.i_org_id
                                            and a.hvr_is_deleted = 0
                                            and b.hvr_is_deleted = 0
                                    )
                                    
                                 END
                            ELSE i.i_schedule_ship_date
                        END
		where ship_date is null;                              
vstatus := 'Step 29 complete';
          update ods_git.nr_shipped_open_order_stg i
        set ship_date =
                    CASE
                        WHEN i.i_flow_status_code = 'CLOSED' OR i.i_flow_status_code = 'SHIPPED' THEN
                            CASE
                                WHEN vfnd_lookup_values <= 0 THEN    
                                i.i_actual_shipment_date
                            END    
                    END
        where ship_date is null;
           vstatus := 'Step 30 complete';
  
         update ods_git.nr_shipped_open_order_stg i
         	set ship_date =  case when i.i_name in (select meaning from sc_ods_evncpad1.applsys.fnd_lookup_values where lookup_type = 'GEAE_CEO_ACCRUAL_OTH_ORDER_TYP' and meaning = i.i_name) 
                                     THEN i.i_booked_date 
                                    WHEN i.i_name in (select meaning from sc_ods_evncpad1.applsys.fnd_lookup_values where lookup_type = 'GEAE_CEO_ACCRUAL_WG_ORDER_TYP' and meaning = i.i_name) 
                                    THEN i.i_booked_date
                                    WHEN i.i_name in (select meaning from sc_ods_evncpad1.applsys.fnd_lookup_values where lookup_type = 'GEAE_CEO_ACCRUAL_WG_ORDER_TYP' and meaning = i.i_name) AND  
                                    to_char(date_trunc('month',sysdate),'DD-MON-YYYY') = to_char(i.i_booked_date,'DD-MON-YYYY') 
                                    then last_day(i.i_booked_date-1)
           END 
		where ship_date is null and (i.i_flow_status_code = 'CLOSED'  OR i.i_flow_status_code = 'SHIPPED') and vfnd_lookup_values <= 0;
vstatus := 'Step 31 complete';
   update ods_git.nr_shipped_open_order_stg i 
   set vsegment3 = 
 (
                        SELECT
                            mc.segment3
                        FROM
                            sc_ods_evncpad1.inv.mtl_item_categories mic,
                            sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                            sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,orgid_tmp v
                        WHERE
                            mcst.category_set_name = 'PRODUCT FINANCE CODE'
                            AND mic.inventory_item_id = i.i_msib_inventory_item_id
                            AND mic.organization_id = i.i_organization_id
                            AND mic.category_set_id = mcst.category_set_id
                            AND mc.category_id = mic.category_id
							AND mic.organization_id = v.organization_id --vorgid -- Added to Test
							AND mic.hvr_is_deleted = 0
							AND mcst.hvr_is_deleted = 0
                    );   
   vstatus := 'Step 32 complete';
update ods_git.nr_shipped_open_order_stg i
			set reverser_nacells = (
                SELECT
                    qp_rn.reversers_nacelles
                FROM
                    sc_ods_evncpad1.cp_mv.q_geae_ceo_om_install_revers qp_rn
                    inner join nr_order_quantity_tmp oqtmp 
                    	on qp_rn.product_finance_code = oqtmp.segment3
                    	and qp_rn.om_order_type = oqtmp.i_tmp_name
                    	and qp_rn.product_finance_code IS NOT null
                where
                	qp_rn.om_order_type = i.i_name
                    and i.i_ool_inventory_item_id = oqtmp.i_tmp_inventory_item_id
                    and i.i_ship_from_org_id = oqtmp.i_tmp_ship_from_org_id
            );
                   vstatus := 'Step 33 complete';
 update ods_git.nr_shipped_open_order_stg i
			set reverser_nacells =(SELECT
                    qp_rn1.reversers_nacelles
                FROM
                    sc_ods_evncpad1.cp_mv.q_geae_ceo_om_install_revers qp_rn1
                WHERE
                    qp_rn1.product_finance_code IS NULL
                    AND qp_rn1.om_order_type = i.i_name
            ) 
           where reverser_nacells is null;
          
          vstatus := 'Step 34 complete';
 /*update ods_git.nr_shipped_open_order_stg i
 set escalation_code =
  (
                SELECT
                    name
                FROM
                    sc_ods_evncpad1.qp.qp_price_formulas_tl
                WHERE
                    price_formula_id = i.i_price_by_formula_id
            )  ;*/
vstatus := 'Step 35 complete';
           update ods_git.nr_shipped_open_order_stg i
 set index1_base =
	 ( SELECT
                    attribute2
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_o2c_error_log geoel1
                WHERE
                    geoel1.request_id = TRUNC(i.i_line_id)::text					
                    AND (geae_em_err_seq_id,TRUNC(i.i_line_id)::text) IN (
                        SELECT
                            MAX(geae_em_err_seq_id),TRUNC(i2.i_line_id)::text
                              from ods_git.nr_shipped_open_order_stg i2,
                            sc_ods_evncpad1.geaecust.geae_em_o2c_error_log
                        WHERE
                            request_id = TRUNC(i2.i_line_id)::text
                            AND attribute1 = 'Index1 Base Value'
                            group by TRUNC(i2.i_line_id)::text
                    )
            )  ;    vstatus := 'Step 36 complete';
       update ods_git.nr_shipped_open_order_stg i
 set index2_base =         
        (
                SELECT
                    attribute2
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_o2c_error_log geoel1
                WHERE
                    geoel1.request_id = TRUNC(i.i_line_id)::text
                    AND (geae_em_err_seq_id,TRUNC(i.i_line_id)::text) IN (
                        SELECT
                            MAX(geae_em_err_seq_id),TRUNC(i2.i_line_id)::text
                        from ods_git.nr_shipped_open_order_stg i2,
                            sc_ods_evncpad1.geaecust.geae_em_o2c_error_log
                        WHERE
                            request_id = TRUNC(i2.i_line_id)::text
                            AND attribute1 = 'Index2 Base Value'
                            group by TRUNC(i2.i_line_id)::text
                    )
            );
           vstatus := 'Step 37 complete';
        update ods_git.nr_shipped_open_order_stg i
        set gta_letter =
             (
                SELECT
                    distinct attribute2
                FROM
                    sc_ods_evncpad1.ont.oe_blanket_headers_all
                WHERE
                    order_number = i.i_blanket_number
                    AND sold_to_org_id = i.i_ooh_sold_to_org_id
                    and hvr_is_deleted = 0
            ) ; vstatus := 'Step 38 complete';
--shop_cost
   update ods_git.nr_shipped_open_order_stg i 
   set vunit_cost1 =
   case when i.i_actual_shipment_date IS null then
                                  (select
                                    a.unit_cost-- into 
													--to_char(a.unit_cost)
                                FROM
                                    sc_ods_evncpad1.cp_mv.q_ceo_future_shop_cost_v a,
                                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                                    sc_ods_evncpad1.ont.oe_order_headers_all oh,
                                    sc_ods_evncpad1.ont.oe_order_lines_all ol
                                WHERE   mic.hvr_is_deleted = 0
									and mcst.hvr_is_deleted = 0							
									and oh.hvr_is_deleted = 0
									and ol.hvr_is_deleted = 0
                                    and mic.category_set_id = mcst.category_set_id
                                    AND mcst.category_set_name = 'CEO_ENGINE_COST'
                                   -- AND mcst.language = userenv('LANG')
                                    AND mc.category_id = mic.category_id
                                    AND a.engine_category = mc.segment1
                                    AND oh.header_id = i.i_source_document_id
                                    AND ol.line_id = i.i_source_document_line_id
                                    AND oh.header_id = ol.header_id
                                    AND mic.organization_id = ol.ship_from_org_id
                                    AND mic.inventory_item_id = ol.inventory_item_id
									--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles') -- Added to Test
                                    AND trunc(ol.schedule_ship_date) BETWEEN nvl(trunc(a.cost_start_date),trunc(ol.schedule_ship_date) - 1) 
                                    AND nvl(trunc(a.cost_end_date),trunc(ol.schedule_ship_date) + 1))
                               end;
                                 vstatus := 'Step 39 complete';                        
                            
					   update  ods_git.nr_shipped_open_order_stg i 
   						set vunit_cost1 =
   							case when	
								i.i_actual_shipment_date IS not null and /*cnt = 0*/ vunit_cost1 is null /*and cnt2 > 0*/ 
								then 
                                (SELECT
                                    SUM(mtacc.base_transaction_value)  --into 
													--to_char(SUM(mtacc.base_transaction_value))
                                FROM
                                    sc_ods_evncpad1.inv.mtl_material_transactions mtt,
                                    sc_ods_evncpad1.inv.mtl_transaction_types mty,
                                    sc_ods_evncpad1.applsys.fnd_lookup_values mta,
                                    sc_ods_evncpad1.inv.mtl_transaction_accounts mtacc,
                                    sc_ods_evncpad1.ont.oe_order_headers_all oh,
                                    sc_ods_evncpad1.ont.oe_order_lines_all ol
                                WHERE
                                    1 = 1
									and mtt.hvr_is_deleted = 0
									and mty.hvr_is_deleted = 0
									and mta.hvr_is_deleted = 0
									and mtacc.hvr_is_deleted = 0							
									and oh.hvr_is_deleted = 0
									and ol.hvr_is_deleted = 0
                                    AND oh.header_id = i.i_source_document_id
                                    AND ol.line_id = i.i_source_document_line_id
                                    AND oh.header_id = ol.header_id
                                    AND mtt.trx_source_line_id = ol.line_id
                                    AND mtt.organization_id = i.i_organization_id
                                    AND mtt.inventory_item_id = i.i_msib_inventory_item_id
                                    AND mty.transaction_type_id = mtt.transaction_type_id
                                    AND mta.lookup_type = 'MTL_TRANSACTION_ACTION'
                                    AND mta.lookup_code = mtt.transaction_action_id
                                    AND mta.meaning = 'COGS Recognition'
                                    AND mtt.transaction_id = mtacc.transaction_id
                                    AND mtacc.accounting_line_type = '35'
									AND mtt.transaction_quantity > 0)
								else
								    0
						end;
				      vstatus := 'Step 40 complete';
					   update  ods_git.nr_shipped_open_order_stg i 
   						set vunit_cost2 =					
					case when i.i_actual_shipment_date IS null then
					  (SELECT
                                   a.unit_cost --into 
											--to_char(a.unit_cost)
                                FROM
                                    sc_ods_evncpad1.cp_mv.q_ceo_future_shop_cost_v a,
                                    sc_ods_evncpad1.inv.mtl_item_categories mic,
                                    sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                                    sc_ods_evncpad1.inv.mtl_category_sets_tl mcst
                                WHERE mic.hvr_is_deleted = 0
									and mcst.hvr_is_deleted = 0	
                                    and mic.category_set_id = mcst.category_set_id
                                    AND mcst.category_set_name = 'CEO_ENGINE_COST'
                                    --AND mcst.language = userenv('LANG')
                                    AND mc.category_id = mic.category_id
                                    AND a.engine_category = mc.segment1
                                    AND mic.organization_id = i.i_ship_from_org_id
                                    AND mic.inventory_item_id = i.i_ool_inventory_item_id
									--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles') -- Added to Test
                                    AND nvl(trunc(i.i_booked_date),trunc(i.i_schedule_ship_date)) BETWEEN nvl(trunc(a.cost_start_date),trunc(i.i_schedule_ship_date) - 1) 
                                    AND nvl(trunc(a.cost_end_date),trunc(i.i_schedule_ship_date) + 1))
                                  end;
                                   vstatus := 'Step 41 complete';
                     update  ods_git.nr_shipped_open_order_stg i 
   						set vunit_cost2 =					
					case when                
                        i.i_actual_shipment_date IS not null and /*cnt3 = 0*/ vunit_cost2 is null /*and cnt4 > 0*/ 
                        then           
								(SELECT
                                    SUM(mtacc.base_transaction_value) --into vunit_cost2
											--to_char(SUM(mtacc.base_transaction_value))
                                FROM
                                    sc_ods_evncpad1.inv.mtl_material_transactions mtt,
                                    sc_ods_evncpad1.inv.mtl_transaction_types mty,
                                    sc_ods_evncpad1.applsys.fnd_lookup_values mta,
                                    sc_ods_evncpad1.inv.mtl_transaction_accounts mtacc
                                WHERE
                                    1 = 1
									and mtt.hvr_is_deleted = 0
									and mty.hvr_is_deleted = 0
									and mta.hvr_is_deleted = 0
									and mtacc.hvr_is_deleted = 0
                                    AND mtt.trx_source_line_id = i.i_line_id
                                    AND mtt.organization_id = i.i_organization_id
                                    AND mtt.inventory_item_id = i.i_msib_inventory_item_id
                                    AND mty.transaction_type_id = mtt.transaction_type_id
                                    AND mta.lookup_type = 'MTL_TRANSACTION_ACTION'
                                    AND mta.lookup_code = mtt.transaction_action_id
                                    AND mta.meaning = 'COGS Recognition'
                                    AND mtt.transaction_id = mtacc.transaction_id
                                    AND mtacc.accounting_line_type = '35'
									AND mtt.transaction_quantity > 0)
					else
					0
				end;
					
         vstatus := 'Step 42 complete';

				 update  ods_git.nr_shipped_open_order_stg i 
   						set vlineno =	
						(SELECT
                                    MIN(oola1.line_number) --into 
                                FROM
                                    sc_ods_evncpad1.ont.oe_order_lines_all oola1
                                WHERE
                                    oola1.header_id = i.i_ooh_header_id
									and oola1.hvr_is_deleted = 0
                                    AND oola1.attribute8 = 'GE Special Equipment'); 
   vstatus := 'Step 43 complete';

  				 update  ods_git.nr_shipped_open_order_stg i 
   						set vcase2basetran =	case when  i.i_line_number = vlineno
                                    THEN
                           (select SUM(nvl(mtacc.base_transaction_value,0) ) --into 
                        FROM
                            sc_ods_evncpad1.inv.mtl_material_transactions mtt,
                            sc_ods_evncpad1.inv.mtl_transaction_types mty,
                            sc_ods_evncpad1.applsys.fnd_lookup_values mta,
                            sc_ods_evncpad1.inv.mtl_transaction_accounts mtacc,
							sc_ods_evncpad1.applsys.fnd_lookup_values flv,
                            sc_ods_evncpad1.inv.mtl_item_categories mic,
                            sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mcv
                        WHERE
                            1 = 1
							and mtt.hvr_is_deleted = 0
									and mty.hvr_is_deleted = 0
									and mta.hvr_is_deleted = 0
									and mtacc.hvr_is_deleted = 0							
									and flv.hvr_is_deleted = 0
									and mic.hvr_is_deleted = 0
                            AND mtt.transaction_source_name = trunc(i.i_order_number)::text
                            AND mty.transaction_type_id = mtt.transaction_type_id
                            AND mta.lookup_type = 'MTL_TRANSACTION_ACTION'
                            AND mcv.concatenated_segments = flv.description
                            AND mcv.category_id = mic.category_id
                            AND mtt.inventory_item_id = mic.inventory_item_id
                            AND flv.lookup_type = 'GEAE_CEO_SHIPDEF_SPARE_LS_ITEM'
                            AND mta.lookup_code = mtt.transaction_action_id
                            AND mta.meaning = 'COGS Recognition'
                            AND mtt.transaction_id = mtacc.transaction_id
                            AND mtacc.accounting_line_type = '35'
							--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles')
							)
							end;
 
             vstatus := 'Step 44 complete';
          

					update  ods_git.nr_shipped_open_order_stg i 
   						set	vunit_cost3 =
							(SELECT ((a.unit_cost::float/a.no_of_engines_shipped::float) * 1) --into unit_cost::INTEGER
								FROM sc_ods_evncpad1.cp_mv.Q_GEAE_CEO_TRUEUPDN_SHOP_COS a,
                                sc_ods_evncpad1.inv.mtl_item_categories mic,
                                sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
                                sc_ods_evncpad1.inv.mtl_category_sets_tl mcst,
                                sc_ods_evncpad1.ont.oe_order_headers_all oh,
                                sc_ods_evncpad1.ont.oe_order_lines_all ol
								WHERE 1 = 1
								    and mic.hvr_is_deleted = 0
									and mcst.hvr_is_deleted = 0
									and oh.hvr_is_deleted = 0							
									and ol.hvr_is_deleted = 0
								AND mic.category_set_id = mcst.category_set_id
								AND mcst.category_set_name = 'CEO_ENGINE_COST'
								--AND mcst.language = userenv('LANG')
								AND mc.category_id = mic.category_id
								AND a.engine_category = mc.segment1
								AND oh.header_id = i.i_source_document_id
								AND ol.line_id = i.i_source_document_line_id
								AND oh.header_id = ol.header_id
								AND mic.organization_id = ol.ship_from_org_id
								AND mic.inventory_item_id = ol.inventory_item_id
								--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles')
								AND mic.organization_id IN (SELECT DISTINCT OOD.ORGANIZATION_ID
															FROM sc_ods_evncpad1.cp_mv.ORG_ORGANIZATION_DEFINITIONS OOD,
																 sc_ods_evncpad1.applsys.FND_LOOKUP_VALUES FLV
															WHERE 1 = 1
															AND OOD.OPERATING_UNIT = FLV.LOOKUP_CODE
															AND FLV.LOOKUP_TYPE = 'GEAE_EM_OPERATING_UNIT'
															AND FLV.ENABLED_FLAG = 'Y'
															and flv.hvr_is_deleted = 0
															--AND FLV.LANGUAGE = USERENV('LANG')
															AND NVL(FLV.END_DATE_ACTIVE,SYSDATE) >= SYSDATE )
								AND trunc(ol.schedule_ship_date) BETWEEN trunc(a.cost_start_date) AND trunc(a.cost_end_date));
							
vstatus := 'Step 45 complete';
							
					update  ods_git.nr_shipped_open_order_stg i 
   						set	vunit_cost4 =		
					(SELECT ((unit_cost::float/no_of_engines_shipped::float) * 1)-- into unit_cost2::integer
								FROM sc_ods_evncpad1.cp_mv.Q_GEAE_CEO_TRUEUPDN_SHOP_COS a,
								sc_ods_evncpad1.inv.mtl_item_categories mic,
								sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc,
								sc_ods_evncpad1.inv.mtl_category_sets_tl mcst
								WHERE 1 = 1
								and mic.hvr_is_deleted = 0
									and mcst.hvr_is_deleted = 0
								AND mic.category_set_id = mcst.category_set_id
								AND mcst.category_set_name = 'CEO_ENGINE_COST'
								--AND mcst.language = userenv('LANG')
								AND mc.category_id = mic.category_id
								AND a.engine_category = mc.segment1
								AND mic.organization_id = i.i_ship_from_org_id
								AND mic.inventory_item_id = i.i_ool_inventory_item_id
								--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles')
								AND mic.organization_id IN (SELECT DISTINCT OOD.ORGANIZATION_ID
															FROM sc_ods_evncpad1.cp_mv.ORG_ORGANIZATION_DEFINITIONS OOD,
																 sc_ods_evncpad1.applsys.FND_LOOKUP_VALUES FLV
															WHERE 1 = 1
															AND OOD.OPERATING_UNIT = FLV.LOOKUP_CODE
															AND FLV.LOOKUP_TYPE = 'GEAE_EM_OPERATING_UNIT'
															AND FLV.ENABLED_FLAG = 'Y'
															and flv.hvr_is_deleted = 0
															--AND FLV.LANGUAGE = USERENV('LANG')
															AND NVL(FLV.END_DATE_ACTIVE,SYSDATE) >= SYSDATE )
								AND trunc(i.i_schedule_ship_date) BETWEEN trunc(a.cost_start_date) AND trunc(a.cost_end_date));
				
vstatus := 'Step 46 complete';
			update  ods_git.nr_shipped_open_order_stg i 
   						set vunit_cost5 =
			(SELECT ((unit_cost::float/no_of_engines_shipped::float) * 1) --into unit_cost3::integer
								FROM sc_ods_evncpad1.cp_mv.Q_GEAE_CEO_TRUEUPDN_SHOP_COS a
								,sc_ods_evncpad1.inv.mtl_item_categories mic
								,sc_ods_evncpad1.cp_mv.mtl_categories_b_kfv mc
								,sc_ods_evncpad1.inv.mtl_category_sets_tl mcst
								WHERE 1 = 1
								and mic.hvr_is_deleted = 0
									and mcst.hvr_is_deleted = 0
								AND mic.category_set_id = mcst.category_set_id
								AND mcst.category_set_name = 'CEO_ENGINE_COST'
								--AND mcst.language = userenv('LANG')
								AND mc.category_id = mic.category_id
								AND a.engine_category = mc.segment1
								AND mic.organization_id = i.i_ship_from_org_id
								AND mic.inventory_item_id = i.i_ool_inventory_item_id
								--AND mic.organization_id = (select organization_id from sc_ods_evncpad1.cp_mv.org_organization_definitions where organization_name = 'CEO - Peebles')
								AND mic.organization_id IN (SELECT DISTINCT OOD.ORGANIZATION_ID
																FROM sc_ods_evncpad1.cp_mv.ORG_ORGANIZATION_DEFINITIONS OOD,
																	 sc_ods_evncpad1.applsys.FND_LOOKUP_VALUES FLV
																WHERE 1 = 1
																AND OOD.OPERATING_UNIT = FLV.LOOKUP_CODE
																AND FLV.LOOKUP_TYPE = 'GEAE_EM_OPERATING_UNIT'
																AND FLV.ENABLED_FLAG = 'Y'
																and flv.hvr_is_deleted = 0
																--AND FLV.LANGUAGE = USERENV('LANG')
																AND NVL(FLV.END_DATE_ACTIVE,SYSDATE) >= SYSDATE )
								AND nvl(i.i_booked_date,trunc(i.i_schedule_ship_date)) BETWEEN trunc(a.cost_start_date) AND trunc(a.cost_end_date));
							
vstatus := 'Step 47 complete';

	update	ods_git.nr_shipped_open_order_stg i 
   		set shop_cost =	      
               nvl(CASE
                    WHEN vfnd_lookup_values > 0 THEN (-1 ) * vunit_cost1::float
                    else vunit_cost2::float
                end,0.00) +
             nvl(CASE
                    WHEN (
                        upper(i.i_ordered_item) LIKE '%BREE'
                        OR upper(i.i_ordered_item) LIKE '%BRAE'
                        OR upper(i.i_ordered_item) LIKE '%BPAE'
                        OR upper(i.i_ordered_item) LIKE '%BRPE'
                    )
                         AND ((nvl(i.i_accounting_rule_id,0.00) = '2000'
                         AND upper(i.i_account_class) = 'REV'
                         AND i.i_user_generated_flag = 'Y')  -- Added for US483251 to pull price as 0 when Accounting Rule is GE_DEFERRED_REV AND 11C Cost will Show in Shop Cost
                         or 
                          (nvl(i.i_accounting_rule_id,0) != '2000'
                          AND upper(i.i_account_class) = 'REV'
                         AND i.i_user_generated_flag != 'Y'))
                          THEN vcase2basetran::float end,0.00) 
                   +						
       
                nvl(CASE
                    WHEN vfnd_lookup_values > 0 THEN (-1 ) * vunit_cost3::float
                    ELSE CASE WHEN i.i_actual_shipment_date IS NOT NULL 
								THEN vunit_cost4::float
				  --     	WHEN i.name in  (select meaning from sc_ods_evncpad1.applsys.fnd_lookup_values where lookup_type = 'GEAE_CEO_ACCRUAL_OTH_ORDER_TYP')
					--							AND i.booked_date is NOT null	
					ELSE						
								vunit_cost5::float
						 end
				END,0.00);
			

vstatus := 'Step 48 complete';
			drop table if exists netsales_tmp;
			create temp table netsales_tmp as
                                        SELECT
                                            count(1),calc.so_line_number,coll.line_type
                                        from sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc2
                                        WHERE
                                            calc2.so_number = calc.so_number
                                        AND coll.rsp_data_seq_id = calc.collection_seq_id
											--AND so_line_number = calc.so_line_number
                                            AND(
                                                CASE
                                                    WHEN calc2.so_line_number IS NOT NULL
                                                         AND calc2.so_line_number = calc.so_line_number THEN 1
                                                    WHEN calc2.so_line_number IS NULL
                                                         AND coll.line_type IN(
                                                        SELECT
                                                            meaning
                                                        FROM
                                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                                        WHERE
                                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                                            AND enabled_flag = 'Y'
															AND hvr_is_deleted = 0
                                                           -- AND language = userenv('LANG')
                                                    ) THEN 1
                                                    ELSE 0
                                                END
                                            ) = 1
                                            AND calc2.calc_type = 'ACTUAL'
                                   group by calc.so_line_number,coll.line_type;

           drop table if exists rsp_sales_sum_tmp;
   		create temp table rsp_sales_sum_tmp as (
                SELECT
                    SUM(rsp_calc.calc_rsp_share_amt) agg_sum,
					--i.i_attribute8,
					i.order_number,i.i_line_id,
					i.i_line_number
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data rsp_coll,
					sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg rsp_ven,
				    sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp_calc,
				    (select distinct * from ods_git.nr_shipped_open_order_stg) i 
                WHERE
                    1 = 1
                   AND rsp_coll.rsp_data_seq_id = rsp_calc.collection_seq_id
                    AND rsp_coll.rsp_data_seq_id = rsp_ven.rsp_data_seq_id
                    AND rsp_calc.vendor_stg_id = rsp_ven.record_id
                    AND rsp_ven.calculation_status = 'FINAL VALID RSP'
                    AND rsp_coll.calculation_status = 'FINAL RSP'
                    AND rsp_calc.calc_status = 'VALID'
					and rsp_coll.hvr_is_deleted = 0 and rsp_ven.hvr_is_deleted = 0 and rsp_calc.hvr_is_deleted = 0
                    AND nvl(rsp_calc.reversal_flag,'N') != 'Y'
                    AND rsp_calc.so_number = i.order_number
                    AND DECODE(rsp_coll.line_type,'GE Revenue Adjustment','GE Manufactured Item',rsp_coll.line_type) IN( 
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                            AND enabled_flag = 'Y'
							AND hvr_is_deleted = 0
                    )
                    AND DECODE(rsp_coll.line_type,'GE Revenue Adjustment','GE Manufactured Item',rsp_coll.line_type) = i.i_attribute8 
                    and (
                        CASE
                            WHEN rsp_calc.so_line_number IS NOT NULL
                                 AND rsp_calc.so_line_number = i.i_line_number THEN 1
                            WHEN rsp_calc.so_line_number IS NULL
                                 AND DECODE(rsp_coll.line_type,'GE Revenue Adjustment','GE Manufactured Item',rsp_coll.line_type) in (
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                    AND enabled_flag = 'Y'
									AND hvr_is_deleted = 0
                            ) THEN 1
                            ELSE 0
                        END
                    ) = 1 
                    AND (case 
                                
                                     WHEN upper(rsp_calc.calc_type) = 'FORECAST'
                                     AND upper(rsp_calc.trx_source) = 'OM'
                                     AND rsp_calc.trx_source_line_id = i.i_line_id  -- Added Line ID Condition for US483251
                                 
                            THEN 1
                                ELSE 0
                            END
                        ) = 1             
        group by
		i.order_number,i.i_line_id,i.i_line_number)		;
	
       
vstatus := 'Step 49 complete';
       
       update  ods_git.nr_shipped_open_order_stg i 
   						set  rsp_sales=  (select agg_sum from rsp_sales_sum_tmp s
   					where i.order_number = s.order_number and i.line_number = s.i_line_number and i.i_line_id = trunc(s.i_line_id)::text);
   				
   						drop table if exists rsp_sales_sum_tmp2;
   					drop table if exists rsp_sales_sum_tmp2;
   						create temp table rsp_sales_sum_tmp2 as
                        SELECT
                            (nvl(SUM(calc.calc_rsp_share_amt),0) * (-1) ) agg_sum ,i.i_source_document_id,
                            i.i_source_document_line_id,i.i_attribute8
                        FROM
                            sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg ven,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
	                       -- ods_git.rsp_tables_jde_line_items_sor rsp_table_1,
                            sc_ods_evncpad1.ont.oe_order_headers_all oh,
                            sc_ods_evncpad1.ont.oe_order_lines_all ol,
                            (select distinct * from ods_git.nr_shipped_open_order_stg) i
                        WHERE
                            1 = 1
                            AND coll.rsp_data_seq_id = calc.collection_seq_id
                            AND coll.rsp_data_seq_id = ven.rsp_data_seq_id
                            AND calc.vendor_stg_id = ven.record_id
                            AND ven.calculation_status = 'FINAL VALID RSP'
                            AND coll.calculation_status = 'FINAL RSP'
                            AND calc.calc_status = 'VALID'
							and coll.hvr_is_deleted = 0 and ven.hvr_is_deleted = 0 and calc.hvr_is_deleted = 0
                            AND nvl(calc.reversal_flag,'N') != 'Y'
                            AND calc.so_number = oh.order_number
                            AND(
                                CASE
                                    WHEN calc.so_line_number IS NOT NULL
                                         AND calc.so_line_number = ol.line_number THEN 1
                                    WHEN calc.so_line_number IS NULL
                                         AND coll.line_type IN(
                                        SELECT
                                            meaning
                                        FROM
                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                        WHERE
                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                            AND enabled_flag = 'Y'
											and hvr_is_deleted = 0
                                           -- AND language = userenv('LANG')
                                    ) THEN 1
                                    ELSE 0
                                END
                            ) = 1
                            AND oh.header_id = i.i_source_document_id
                            AND ol.line_id = i.i_source_document_line_id
                            AND oh.header_id = ol.header_id
                            AND coll.line_type = i.i_attribute8
                            AND coll.line_type IN(
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                    AND enabled_flag = 'Y'
									and hvr_is_deleted = 0
                            ) 
                            AND(
                                CASE
                                    WHEN EXISTS(
                                        SELECT
                                            1
                                        FROM
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc
                                        WHERE
                                            so_number = calc.so_number
											and hvr_is_deleted = 0
											--AND so_line_number = calc.so_line_number
                                            AND(
                                                CASE
                                                    WHEN so_line_number IS NOT NULL
                                                         AND so_line_number = calc.so_line_number THEN 1
                                                    WHEN so_line_number IS NULL
                                                         AND coll.line_type IN(
                                                        SELECT
                                                            meaning
                                                        FROM
                                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                                        WHERE
                                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                                            AND enabled_flag = 'Y'
															and hvr_is_deleted = 0
                                                            --AND language = userenv('LANG')
                                                    ) THEN 1
                                                    ELSE 0
                                                END
                                            ) = 1
                                            AND calc_type = 'ACTUAL'
                                    )
                                         AND calc.calc_type = 'ACTUAL' THEN 1
                                    WHEN NOT EXISTS( 
                                        SELECT
                                            1
                                        FROM
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc
                                        WHERE
                                            so_number = calc.so_number
											and hvr_is_deleted = 0
											--AND so_line_number = calc.so_line_number
                                            AND(
                                                CASE
                                                    WHEN so_line_number IS NOT NULL
                                                         AND so_line_number = calc.so_line_number THEN 1
                                                    WHEN so_line_number IS NULL
                                                         AND coll.line_type IN(
                                                        SELECT
                                                            meaning
                                                        FROM
                                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                                        WHERE
                                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                                            AND enabled_flag = 'Y'
															and hvr_is_deleted = 0
                                                            --AND language = userenv('LANG')
                                                    ) THEN 1
                                                    ELSE 0
                                                END
                                            ) = 1
                                            AND calc_type = 'ACTUAL'
                                    )
                                             AND calc.calc_type = 'FORECAST' THEN 1
                                    ELSE 0
                                END
                            ) = 1
                            group by i.i_source_document_id,
                            i.i_source_document_line_id,i.i_attribute8;
                      
          update  ods_git.nr_shipped_open_order_stg i 
   						set  rsp_sales=    CASE
                    WHEN vfnd_lookup_values > 0 THEN nvl( (select agg_sum from rsp_sales_sum_tmp2 s
   					where i.i_source_document_id = s.i_source_document_id and i.i_attribute8 = s.i_attribute8
						and i.i_source_document_line_id = s.i_source_document_line_id),0) else 0 end
					where   i.rsp_sales is null;                   
                           
                     /*      select * from rsp_sales_sum_tmp2
                           where order_number = 100005*/
                           
		 vstatus := 'Step 50 complete';
            
        update  ods_git.nr_shipped_open_order_stg i 
   						set  rsp_sales=  rsp_sales + nvl((SELECT                   -- Added for US636155 Starts
						SUM(rsp.calc_rsp_share_amt)
						FROM
						sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp
						WHERE 1 = 1
						AND rsp.calc_status = 'VALID'
						AND upper(rsp.calc_type) = 'FORECAST'
						AND upper(rsp.trx_source) = 'MISC'
						AND rsp.so_number = i.order_number
						AND rsp.so_line_number = i.line_number
						AND rsp.hvr_is_deleted = 0 
						AND DECODE(rsp.item_type,'GE Revenue Adjustment','GE Manufactured Item',rsp.item_type) IN( SELECT meaning
																													FROM sc_ods_evncpad1.applsys.fnd_lookup_values
																													WHERE 1 = 1
																													AND lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
																													AND enabled_flag = 'Y'
																													AND hvr_is_deleted = 0
																													--AND language = userenv('LANG')
																												)),0);
					vstatus := 'Step 51 complete';																						


drop table if exists meaning_tmp;

create  temp table  meaning_tmp   as                    (
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type IN(
                                'GEAE_CEO_DISCOUNT_ITEM_TYPE'
                            )
                            AND enabled_flag = 'Y'
							and hvr_is_deleted = 0
                            --AND language = userenv('LANG')
                    );      
                 
             analyse meaning_tmp;      
                           
        drop table if exists netsales_tmp;                   
       create temp table netsales_tmp as
                                        SELECT
                                            count(1),calc.so_line_number,coll.line_type
                                            from sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                                           sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc2
                                        WHERE
                                            calc2.so_number = calc.so_number
                                            AND coll.rsp_data_seq_id = calc.collection_seq_id
											--AND so_line_number = calc.so_line_number
                                            AND(
                                                CASE
                                                    WHEN calc2.so_line_number IS NOT NULL
                                                         AND calc2.so_line_number = calc.so_line_number THEN 1
                                                    WHEN calc2.so_line_number IS NULL
                                                         AND coll.line_type IN(
                                                        SELECT
                                                            meaning
                                                        FROM
                                                            meaning_tmp
                                                    ) THEN 1
                                                    ELSE 0
                                                END
                                            ) = 1
                                            AND calc2.calc_type = 'ACTUAL'
                                   group by calc.so_line_number,coll.line_type;   
                                  
                                  
                 analyse netsales_tmp;                         
 drop table if exists inv_line_tmp; 
                   create temp table inv_line_tmp as (
                        select i.i_ooh_header_id,i.i_ool_header_id,i.i_org_id,i.line_number,i.order_number,rsp_coll.source_trx_line_id,  
						nvl(case when position('.' in rctla.sales_order_line::text) > 0
                                       then split_part(rctla.sales_order_line::text,'.',1)::numeric
                                 else rctla.sales_order_line::numeric end,                           
						(SELECT MIN(oola1.line_number)
						FROM sc_ods_evncpad1.ont.oe_order_lines_all oola1 
						WHERE oola1.header_id = i.i_ooh_header_id 
						AND oola1.attribute8 = 'GE Discount Item'
						and oola1.hvr_is_deleted = 0)						
						)::numeric inv_line -- Newly Added to pull the Line Number
                        FROM
                            sc_ods_evncpad1.ar.ra_customer_trx_all rcta,
                            sc_ods_evncpad1.ar.ra_customer_trx_lines_all rctla,
                            sc_ods_evncpad1.ar.ra_cust_trx_types_all rctt,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data rsp_coll,
                            ods_git.nr_shipped_open_order_stg i
                        WHERE
                            rcta.customer_trx_id = rctla.customer_trx_id
							and rctt.hvr_is_deleted = 0
                            AND rctt.name IN(
                                SELECT
                                    description
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values flv
                                WHERE
                                    lookup_type = 'GEAE_EM_TRX_TYPES'
                                    AND "tag" IS NULL
                                    AND enabled_flag = 'Y'
									and hvr_is_deleted = 0
                                  --  AND language = userenv('LANG')
                            )
                            AND rctt.cust_trx_type_id = rcta.cust_trx_type_id
                           -- AND i.i_ooh_header_id = i.i_ool_header_id
                            AND rctla.customer_trx_line_id = rsp_coll.source_trx_line_id--SB
                            AND rctla.sales_order = trunc(i.order_number)::text
                            and nvl(case when position('.' in rctla.sales_order_line::text) > 0
                                       then split_part(rctla.sales_order_line::text,'.',1)::numeric
                                 else rctla.sales_order_line::numeric end,
                            (SELECT MIN(oola1.line_number)
                            FROM sc_ods_evncpad1.ont.oe_order_lines_all oola1 
                            WHERE oola1.header_id = i.i_ooh_header_id 
							AND oola1.attribute8 = 'GE Discount Item'
							and oola1.hvr_is_deleted = 0))::numeric = (i.line_number)  -- Newly Added to pull the Line Number
                            AND i.i_org_id = rcta.org_id);
                           
                           analyse inv_line_tmp;
             
                            vstatus := 'Step 54 complete';	
                        
                      analyse inv_line_tmp;         
                     
                 drop table if exists inv_line2_tmp;
                           create temp table inv_line2_tmp as         (
                        SELECT
                            MIN(oola1.line_number) line_number,i.i_ooh_header_id
                        FROM
                            sc_ods_evncpad1.ont.oe_order_lines_all oola1,
                            ods_git.nr_shipped_open_order_stg i
                        WHERE
                            oola1.header_id = i.i_ooh_header_id
                            AND oola1.attribute8 = 'GE Discount Item'
							and hvr_is_deleted = 0
                            group by i.i_ooh_header_id
                    );
                   
                    vstatus := 'Step 55 complete';	
                       analyse inv_line2_tmp;   
                      
                   drop table  if exists cnt_tmp;
                   create temp table cnt_tmp as
                   (
                                    SELECT
                                        count(1) cnt1,
                                        i.i_ooh_header_id,i.i_ool_header_id,i.i_ool_inventory_item_id,
                                        i.i_line_number,i.i_org_id,i.i_order_number
                                    FROM
                                        sc_ods_evncpad1.ar.ra_customer_trx_all rcta,
                                        sc_ods_evncpad1.ar.ra_customer_trx_lines_all rcl,
                                        sc_ods_evncpad1.ar.ar_payment_schedules_all aps,
                                        sc_ods_evncpad1.ar.ra_cust_trx_types_all rtt,
                                        ods_git.nr_shipped_open_order_stg i
                                    WHERE
                                        aps.customer_trx_id = rcta.customer_trx_id
                                        AND i.i_ooh_header_id = i.i_ool_header_id
                                        AND i.i_ool_inventory_item_id = rcl.inventory_item_id
                                        AND rcta.customer_trx_id = rcl.customer_trx_id
                                        AND rcta.cust_trx_type_id = rtt.cust_trx_type_id
                                        AND (i.line_number) = rcl.sales_order_line
                                        AND rcta.org_id = rtt.org_id
                                        AND rcta.org_id = i.i_org_id
										--AND rcta.trx_number = nvl(p_trx_number, rcta.trx_number)
                                        AND rcta.interface_header_attribute1 = trunc(i.i_order_number)::text
										-- AND rcl.sales_order = ooh.order_number
										-- AND rcl.interface_line_attribute6 = trunc(i.i_line_id)::text
                                        AND aps.org_id = i.i_org_id
                                        AND nvl(i.i_attribute9,'N') = 'Y'
                                        AND i.i_ool_context = 'CEO'
                                        AND aps.class = 'INV'
                                        AND rcl.revenue_amount != 0
                                        AND aps.amount_due_remaining = 0
                                        group by i.i_ooh_header_id,i.i_ool_header_id,i.i_ool_inventory_item_id,
                                        i.i_line_number,i.i_org_id,i.i_order_number
                                );
                                vstatus := 'Step 56 complete';	
                       analyse cnt_tmp;   
              
                      drop table if exists grc_tmp;
             create temp table grc_tmp as (
                                    SELECT
                                        count(1) as cnt3,rsp_calc.so_line_number,rsp_calc.so_number
                                    FROM
                                        sc_ods_evncpad1.geaecust.geae_em_rsp_calc grc,
                                        sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp_calc
                                    WHERE
                                        upper(grc.trx_source) = 'AR'
                                        AND grc.so_number = rsp_calc.so_number
                                        AND grc.so_line_number = rsp_calc.so_line_number
                                        AND upper(grc.calc_type) = 'ACTUAL'
                                        AND grc.calc_status = 'VALID'
                                        group by rsp_calc.so_line_number,rsp_calc.so_number
                                );
                                vstatus := 'Step 57 complete';	
                      analyse grc_tmp;                 
      
  	
  				drop table if exists rsp_concession_add_sum_tmp;
   create temp table rsp_concession_add_sum_tmp as  
   select sum(agg_sum) agg_sum,i_attribute8,order_number,i_line_number,i_ool_header_id from
   (
                select distinct
                   --sum 
                   (rsp_calc.calc_rsp_share_amt) agg_sum,i.i_attribute8,i.order_number,i.i_line_number,i.i_ool_header_id
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data rsp_coll,
                    sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg rsp_ven,
                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp_calc,
                    meaning_tmp m,(select distinct * from inv_line_tmp) inv
                    ,inv_line2_tmp inv2,cnt_tmp,grc_tmp
                    ,ods_git.nr_shipped_open_order_stg i
                WHERE
                    1 = 1
                    AND rsp_coll.rsp_data_seq_id = rsp_calc.collection_seq_id
                    AND rsp_coll.rsp_data_seq_id = rsp_ven.rsp_data_seq_id
                    AND rsp_calc.vendor_stg_id = rsp_ven.record_id
                    AND rsp_ven.calculation_status = 'FINAL VALID RSP'
                    AND rsp_coll.calculation_status = 'FINAL RSP'
                    AND rsp_calc.calc_status = 'VALID'
					and rsp_coll.hvr_is_deleted = 0 and rsp_ven.hvr_is_deleted = 0 and rsp_calc.hvr_is_deleted = 0
                    AND nvl(rsp_calc.reversal_flag,'N') != 'Y'  -- Added for US483251 not to pull offset records					
                    AND i.order_number=rsp_calc.so_number
                    and i.i_ooh_header_id =inv.i_ooh_header_id(+)
                    and trunc(i.line_number)::text =trunc(inv.line_number(+))::text
                    AND trunc(i.order_number)::text=trunc(inv.order_number(+))::text
                   -- and rsp_coll.source_trx_line_id=inv.source_trx_line_id(+)
                    and i.i_ooh_header_id=inv2.i_ooh_header_id(+)
                      AND i.i_ooh_header_id=cnt_tmp.i_ooh_header_id(+)
                                     and i.i_ool_header_id=cnt_tmp.i_ool_header_id(+)
									 and i.i_ool_inventory_item_id=cnt_tmp.i_ool_inventory_item_id(+)
                                     and i.i_line_number=cnt_tmp.i_line_number(+)
                                     and i.i_org_id=cnt_tmp.i_org_id(+)
                                     and i.i_order_number=cnt_tmp.i_order_number(+)
                                      and rsp_calc.so_line_number=grc_tmp.so_line_number(+)
									 and rsp_calc.so_number=grc_tmp.so_number(+)
                    AND nvl(trunc(rsp_calc.so_line_number)::numeric,                   
                    nvl(inv.inv_line, inv2.line_number
                     ) ) = trunc(i.i_line_number)
                    AND rsp_coll.line_type = m.meaning
                    AND rsp_coll.line_type = i.i_attribute8
                    AND(
                        CASE
                            WHEN rsp_calc.so_line_number IS NOT NULL
                                 AND rsp_calc.so_line_number = i.i_line_number THEN 1
                            WHEN rsp_calc.so_line_number IS NULL
                                 AND rsp_coll.line_type = m.meaning THEN 1
                            ELSE 0
                        END
                    ) = 1
                    AND (
                            CASE
                                WHEN nvl(cnt1,0) > 0 
                                     AND upper(rsp_calc.calc_type) = 'ACTUAL'
                                     AND upper(rsp_calc.trx_source) = 'AR' THEN 1					
                                WHEN upper(rsp_calc.calc_type) = 'FORECAST'
                                     AND upper(rsp_calc.trx_source) = 'OM'                                   
                                     AND rsp_calc.trx_source_line_id = i.i_line_id  -- Added Line ID Condition for US483251                                    
                                     AND nvl(grc_tmp.cnt3,0) = 0 THEN 1
                                ELSE 0
                            END
                        ) = 1           
           )
           group by i_attribute8,order_number,i_line_number,i_ool_header_id;    
           
           analyse rsp_concession_add_sum_tmp;
              	 update  ods_git.nr_shipped_open_order_stg i 
   						set  rsp_concessions1=  (select agg_sum from rsp_concession_add_sum_tmp s
   					where i.order_number = s.order_number and i.i_line_number = s.i_line_number and i.i_attribute8 = s.i_attribute8
   				and s.i_ool_header_id = i.i_ool_header_id);
                           
   			            update  ods_git.nr_shipped_open_order_stg i 
   	set rsp_concessions1 = 
            CASE
                    WHEN vfnd_lookup_values > 0 THEN nvl( (
                        SELECT
                            (nvl(SUM(calc.calc_rsp_share_amt),0) * (-1) )
                        FROM
                            sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg ven,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                            sc_ods_evncpad1.ont.oe_order_headers_all oh,
                            sc_ods_evncpad1.ont.oe_order_lines_all ol,
                            netsales_tmp t
                        WHERE
                            1 = 1
                            AND coll.rsp_data_seq_id = calc.collection_seq_id
                            AND coll.rsp_data_seq_id = ven.rsp_data_seq_id
                            AND calc.vendor_stg_id = ven.record_id
                            AND ven.calculation_status = 'FINAL VALID RSP'
                            AND coll.calculation_status = 'FINAL RSP'
                            AND calc.calc_status = 'VALID'
                            AND nvl(calc.reversal_flag,'N') != 'Y'
                            AND calc.so_number = oh.order_number
                            AND(
                                CASE
                                    WHEN calc.so_line_number IS NOT NULL
                                         AND calc.so_line_number = ol.line_number THEN 1
                                    WHEN calc.so_line_number IS NULL
                                         AND coll.line_type IN(
                                        SELECT
                                            meaning
                                        FROM
                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                        WHERE
                                            lookup_type = 'GEAE_CEO_ENGINE_ITEM_TYPE'
                                            AND enabled_flag = 'Y'
											and hvr_is_deleted = 0
                                           -- AND language = userenv('LANG')
                                    ) THEN 1
                                    ELSE 0
                                END
                            ) = 1
                            AND oh.header_id = i.i_source_document_id
                            AND ol.line_id = i.i_source_document_line_id
                            AND oh.header_id = ol.header_id
                            AND coll.line_type = i.i_attribute8
                            AND coll.line_type IN(
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type IN(
                                        'GEAE_CEO_DISCOUNT_ITEM_TYPE'
                                    )
                                    AND enabled_flag = 'Y'
									AND hvr_is_deleted = 0
                                  --  AND language = userenv('LANG')
                            )
                            and (case when t.so_line_number= calc.so_line_number
                            and t.line_type = coll.line_type and t.count > 0
                            AND calc.calc_type = 'ACTUAL' THEN 1
                             when t.so_line_number= calc.so_line_number
                            and t.line_type = coll.line_type and t.count =0
                            AND calc.calc_type = 'FORECAST' THEN 1 else 0 end)= 1
                    ),0)
                    ELSE 0
                END
         where  rsp_concessions1 is null;  -- into rsp_concessions1;
        
         vstatus := 'Step 59 complete';	

      --analyse line_no_tmp;   
  drop table if exists meaning_tmp;
   create  temp table  meaning_tmp   as                    (
                        SELECT
                            meaning
                        FROM
                            sc_ods_evncpad1.applsys.fnd_lookup_values
                        WHERE
                            lookup_type IN(
                                'GEAE_CEO_DISCOUNT_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                            )
                            AND enabled_flag = 'Y'
							AND hvr_is_deleted = 0
                            --AND language = userenv('LANG')
                    );
                   
     drop table if exists line_no_tmp;   
    create temp table line_no_tmp as
	(SELECT MIN(oola1.line_number) lno,i.i_ooh_header_id,oola1.attribute8 
	FROM sc_ods_evncpad1.ont.oe_order_lines_all oola1, ods_git.nr_shipped_open_order_stg i 
                        WHERE oola1.header_id = i.i_ooh_header_id
						and oola1.hvr_is_deleted = 0
						--AND oola1.attribute8 in (Select meaning from meaning_tmp)
                       group by i.i_ooh_header_id,oola1.attribute8);
					   
                    vstatus := 'Step 60 complete';	
         analyse meaning_tmp; 
 drop table if exists netsales_tmp;                   
       create temp table netsales_tmp as
                                        SELECT
                                            count(1),calc.so_number,calc.so_line_number,coll.line_type
                                        from sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc2
                                        WHERE
                                            calc2.so_number = calc.so_number
											and calc.hvr_is_deleted = 0
											and calc2.hvr_is_deleted = 0
											and coll.hvr_is_deleted = 0
                                            AND coll.rsp_data_seq_id = calc.collection_seq_id
											AND   coll.calculation_status = 'FINAL RSP'	
											AND   calc.calc_status = 'VALID'
											--AND so_line_number = calc.so_line_number
                                            AND(
                                                CASE
                                                    WHEN calc2.so_line_number IS NOT NULL
                                                         AND calc2.so_line_number = calc.so_line_number THEN 1
                                                    WHEN calc2.so_line_number IS NULL
                                                         AND coll.line_type in (select meaning from meaning_tmp
                                                    ) THEN 1
                                                    ELSE 0
                                                END
                                            ) = 1
                                            AND calc2.calc_type = 'ACTUAL'
                                   group by calc.so_number,calc.so_line_number,coll.line_type;   
                                  
                                  vstatus := 'Step 61 complete';	
    analyse netsales_tmp; 
    drop table if exists actual_tmp;
    create temp table actual_tmp
    as ( 
                                SELECT
                                    count(1) cnt_tmp,i.i_order_number,i.i_line_number,i.i_org_id
                                FROM
                                    sc_ods_evncpad1.ap.ap_invoices_all aia,
                                    sc_ods_evncpad1.ap.ap_invoice_lines_all ail,
                                    ods_git.nr_shipped_open_order_stg i
                                WHERE
                                    aia.invoice_id = ail.invoice_id
									and aia.hvr_is_deleted = 0
									and ail.hvr_is_deleted = 0
                                    AND ail.attribute_category = 'CEO'
                                    AND ail.org_id = i.i_org_id
                                    AND ail.attribute1 = trunc(i.i_order_number)::text 
                                    AND ail.attribute2 = trunc(i.i_line_number)::text
									AND (aia.source = 'GEAE_CEO_RSP' OR aia.source = 'GEAE_EM_CONC')  -- Added Newly to pull Line Number on 12-Aug-2021
                                    AND aia.org_id = '122619'--v_ge_ceo_org
                                    group by i.i_order_number,i.i_line_number,i.i_org_id
                            );
                           
                           vstatus := 'Step 62 complete';	
      analyse actual_tmp; 
    drop table if exists forecast_tmp;
    create temp table forecast_tmp
    as (
                                SELECT
                                    count(1) cnt_tmp,rsp_calc.so_number,rsp_calc.so_line_number
                                FROM
                                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc grc,
                                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp_calc
                                WHERE
                                    upper(grc.trx_source) = 'AP'
                                    AND grc.so_number = rsp_calc.so_number
                                    AND grc.so_line_number = rsp_calc.so_line_number
                                    AND upper(grc.calc_type) = 'ACTUAL'
                                    AND grc.calc_status = 'VALID'
									and grc.hvr_is_deleted = 0
									and rsp_calc.hvr_is_deleted = 0
                                    group by rsp_calc.so_number,rsp_calc.so_line_number
                            );
                  vstatus := 'Step 63 complete';	
          analyse forecast_tmp;                 

   	drop table if exists rsp_concession_add_sum_tmp2;
   create temp table rsp_concession_add_sum_tmp2 as
			select sum(calc_rsp_share_amt) agg_sum,attribute8,order_number,line_number,i_ool_header_id from (
                SELECT
                distinct    (rsp_calc.calc_rsp_share_amt) ,i.attribute8,i.order_number,i.line_number,i.i_ool_header_id
                ,actual.cnt_tmp,upper(rsp_calc.calc_type),upper(rsp_calc.trx_source)
                FROM
                    sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data rsp_coll,
                    sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg rsp_ven,
                    sc_ods_evncpad1.geaecust.geae_em_rsp_calc rsp_calc,
                    --meaning_tmp m, 
                    line_no_tmp l, actual_tmp actual, forecast_tmp f,
                    ods_git.nr_shipped_open_order_stg i 
                WHERE
                    1 = 1
                    AND rsp_coll.rsp_data_seq_id = rsp_calc.collection_seq_id
                    AND rsp_coll.rsp_data_seq_id = rsp_ven.rsp_data_seq_id
                    AND rsp_calc.vendor_stg_id = rsp_ven.record_id
                    AND rsp_ven.calculation_status = 'FINAL VALID RSP'
                    AND rsp_coll.calculation_status = 'FINAL RSP'
                    AND rsp_calc.calc_status = 'VALID'
					and rsp_coll.hvr_is_deleted = 0 and rsp_ven.hvr_is_deleted = 0 and rsp_calc.hvr_is_deleted = 0
                    AND nvl(rsp_calc.reversal_flag,'N') != 'Y'
                    AND rsp_calc.so_number = i.order_number
                    and l.i_ooh_header_id=i.i_ooh_header_id
                    and l.attribute8 in (SELECT meaning
										FROM sc_ods_evncpad1.applsys.fnd_lookup_values
										WHERE  lookup_type IN('GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE' )
										AND enabled_flag = 'Y'
										AND hvr_is_deleted = 0)
					AND nvl(rsp_calc.so_line_number,l.lno) = i.line_number
                    --AND rsp_coll.line_type = m.meaning                        
					and rsp_coll.line_type in (
							SELECT
								meaning
							FROM
								sc_ods_evncpad1.applsys.fnd_lookup_values
							WHERE
								lookup_type in (
									'GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
								)
                            AND enabled_flag = 'Y'
                            and hvr_is_deleted = 0
						)
                    AND rsp_coll.line_type = i.attribute8
                    AND(
                        CASE
                            WHEN rsp_calc.so_line_number IS NOT NULL
                                 AND rsp_calc.so_line_number = i.line_number THEN 1
                            WHEN rsp_calc.so_line_number IS NULL
                                 AND rsp_coll.line_type  IN
									 (
										SELECT
											meaning
										FROM
											sc_ods_evncpad1.applsys.fnd_lookup_values
										WHERE
											lookup_type IN(
												'GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
											)
										AND enabled_flag = 'Y'
										and hvr_is_deleted = 0
									)  THEN 1
                            ELSE 0
                        END
                    ) = 1
                    and trunc(i.i_order_number)::text = trunc(actual.i_order_number(+))::text and trunc(i.i_line_number)::text = trunc(actual.i_line_number(+))::text
                    and i.i_org_id=actual.i_org_id(+) 
                    and rsp_calc.so_number = f.so_number(+) and rsp_calc.so_line_number=f.so_line_number(+)  and rsp_calc.so_line_number=f.so_line_number(+) and
                    (case when nvl(actual.cnt_tmp,0) > 0 AND upper(rsp_calc.calc_type) = 'ACTUAL'
                                 AND upper(rsp_calc.trx_source) = 'AP' THEN 1                                  
                           when nvl(f.cnt_tmp,0) = 0 and       
                          upper(rsp_calc.calc_type) = 'FORECAST'
                                 AND upper(rsp_calc.trx_source) = 'OM'
                                 AND rsp_calc.trx_source_line_id = i.i_line_id -- Added Line ID Condition for US483251
                                  THEN 1
                            ELSE 0
                        END
                    ) = 1
            ) group by attribute8,order_number,line_number,i_ool_header_id;
           
           
           	update  ods_git.nr_shipped_open_order_stg i 
   	set rsp_concessions2 =   (select agg_sum from rsp_concession_add_sum_tmp2 s
   					where i.order_number = s.order_number and i.line_number = s.line_number and i.attribute8 = s.attribute8
   				and s.i_ool_header_id = i.i_ool_header_id);

   	
vstatus := 'Step 64 complete';	
	update  ods_git.nr_shipped_open_order_stg i 
   	set rsp_concessions2 = CASE
                    WHEN vfnd_lookup_values > 0 THEN nvl( (
                        SELECT
                            (SUM(calc.calc_rsp_share_amt) * (-1) )
                        FROM
                            sc_ods_evncpad1.geaecust.geae_em_rsp_collection_data coll,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_vendors_stg ven,
                            sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc,
                            sc_ods_evncpad1.ont.oe_order_headers_all oh,
                            sc_ods_evncpad1.ont.oe_order_lines_all ol,
                            netsales_tmp t
                        WHERE
                            1 = 1
                            AND coll.rsp_data_seq_id = calc.collection_seq_id
                            AND coll.rsp_data_seq_id = ven.rsp_data_seq_id
                            AND calc.vendor_stg_id = ven.record_id
                            AND ven.calculation_status = 'FINAL VALID RSP'
                            AND coll.calculation_status = 'FINAL RSP'
                            AND calc.calc_status = 'VALID'
							and coll.hvr_is_deleted = 0 and ven.hvr_is_deleted = 0 and calc.hvr_is_deleted = 0
                            AND nvl(calc.reversal_flag,'N') != 'Y'
                            AND calc.so_number = oh.order_number
                            AND(
                                CASE
                                    WHEN calc.so_line_number IS NOT NULL
                                         AND calc.so_line_number = ol.line_number THEN 1
                                    WHEN calc.so_line_number IS NULL
                                         AND coll.line_type IN(
                                        SELECT
                                            meaning
                                        FROM
                                            sc_ods_evncpad1.applsys.fnd_lookup_values
                                        WHERE
                                            lookup_type IN(
                                                'GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                                            )
                                            AND enabled_flag = 'Y'
                                            --AND language = userenv('LANG')
                                    ) THEN 1
                                    ELSE 0
                                END
                            ) = 1
                            AND oh.header_id = i.i_source_document_id
                            AND ol.line_id = i.i_source_document_line_id
                            AND oh.header_id = ol.header_id
							and oh.hvr_is_deleted = 0
							and ol.hvr_is_deleted = 0
                            AND coll.line_type = i.i_attribute8
                            AND coll.line_type IN(
                                SELECT
                                    meaning
                                FROM
                                    sc_ods_evncpad1.applsys.fnd_lookup_values
                                WHERE
                                    lookup_type IN(
                                        'GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE'
                                    )
                                    AND enabled_flag = 'Y'
                                  --  AND language = userenv('LANG')
                            )
                            and (case when 
								t.so_number = calc.so_number
							and t.so_line_number= calc.so_line_number
                            and t.line_type = coll.line_type and t.count > 0
                            AND calc.calc_type = 'ACTUAL' THEN 1
                             when t.so_number = calc.so_number
							and t.so_line_number= calc.so_line_number
                            and t.line_type = coll.line_type and t.count =0
                            AND calc.calc_type = 'FORECAST' THEN 1 else 0 end) = 1
                    ),0)
                    ELSE 0
                end
       where  rsp_concessions2 is null or  rsp_concessions2 = 0;
         
	vstatus := 'Step 65 complete';	

/*	if 
cnt_tmp>0 then */ 
      update  ods_git.nr_shipped_open_order_stg i 
   	set rsp_concessions3 =  
       nvl( (SELECT                   -- Added for US636155 Starts
						SUM(calc.calc_rsp_share_amt)
						FROM
						sc_ods_evncpad1.geaecust.geae_em_rsp_calc calc
						WHERE 1 = 1
						AND calc.calc_status = 'VALID'
						AND upper(calc.calc_type) = 'FORECAST'
						AND upper(calc.trx_source) = 'MISC'
						AND calc.so_number = i.order_number
						AND calc.so_line_number = i.line_number
						AND calc.hvr_is_deleted = 0
						AND calc.item_type IN( SELECT meaning
													 FROM sc_ods_evncpad1.applsys.fnd_lookup_values
													 WHERE 1 = 1
													 AND lookup_type IN('GEAE_CEO_CONCESSION_ITEM_TYPE','GEAE_CEO_CONTRCT_TECH_GUR_TYPE','GEAE_CEO_DISCOUNT_ITEM_TYPE')
													 AND enabled_flag = 'Y'
													-- AND language = userenv('LANG')
													 )),0);-- into rsp_concessions3;
													
													vstatus := 'Step 66 complete';	
 update  ods_git.nr_shipped_open_order_stg i 
 set
rsp_concessions =	i.rsp_concessions1::decimal + i.rsp_concessions2::decimal + i.rsp_concessions3::decimal;																											

         vstatus := 'Step 67 complete';	
        
        
  
update ods_git.nr_shipped_open_order_stg t   
set ship_year=(to_char(t.ship_date,'YYYY'))::int,
ship_quarter=(to_char(t.ship_date,'Q')),
ship_month=(to_char(t.ship_date,'MON-YY'))
from ods_git.nr_shipped_open_order_final_table_stg i
where t.order_number=i.order_number
and t.item_code= i.segment1
and t.i_line_id=i.line_id
and t.i_ool_header_id=i.ool_header_id
--and t.i_ooh_header_id-i.ooh_header_id
and t.i_msib_inventory_item_id=i.msib_inventory_item_id
and t.i_organization_id=i.organization_id
and t.i_ool_inventory_item_id=i.ool_inventory_item_id
; 

--drop table ods_git.nr_shipped_open_order_stg1;
CREATE TABLE IF NOT EXISTS ods_git.nr_cp_shipped_open_order_stg1
(
	department VARCHAR(720)   ENCODE lzo
	,engine_family VARCHAR(256)   ENCODE lzo
	,engine_model VARCHAR(256)   ENCODE lzo
	,order_type VARCHAR(256)   ENCODE lzo
	,sub_family VARCHAR(256)   ENCODE lzo
	,invoice_number VARCHAR(256)   ENCODE lzo
	--,contract_name VARCHAR(256)   ENCODE lzo
	,ultimate_customer VARCHAR(256)   ENCODE lzo
	--,ultimate_customer_number VARCHAR(256)   ENCODE lzo
	,install_spare VARCHAR(256)   ENCODE lzo
	,product_type VARCHAR(256)   ENCODE lzo
	--,ordered_quantity NUMERIC(38,4)   ENCODE az64
	--,unit_selling_price NUMERIC(38,4)   ENCODE az64
	--,sales_old NUMERIC(38,4)   ENCODE az64
	,sales NUMERIC(38,4)   ENCODE az64
	--,concessions_old NUMERIC(38,4)   ENCODE az64
	,concessions NUMERIC(38,4)   ENCODE az64
	--,concessions_discount_old NUMERIC(38,4)   ENCODE az64
	,concessions_discount NUMERIC(38,4)   ENCODE az64
	,net_sales NUMERIC(38,4)   ENCODE az64
	--,net_sales_old NUMERIC(38,4)   ENCODE az64
	,order_number NUMERIC(38,4)   ENCODE az64
	,line_number NUMERIC(38,4)   ENCODE az64
	--,cust_po_number VARCHAR(256)   ENCODE lzo
	--,item_code VARCHAR(256)   ENCODE lzo
	--,description VARCHAR(256)   ENCODE lzo
	,concessions_ap_misc NUMERIC(38,4)   ENCODE az64
	,shop_cost VARCHAR(720)   ENCODE lzo
	,rsp_sales VARCHAR(720)   ENCODE lzo
	,rsp_concessions VARCHAR(720)   ENCODE lzo
	--,rsp_concessionf VARCHAR(720)   ENCODE lzo
	--,rsp_concession_actual_ap VARCHAR(720)   ENCODE lzo
	--,rsp_concession_actual_misc_ar VARCHAR(720)   ENCODE lzo
	--,ordered_date DATE   ENCODE az64
	--,promise_date DATE   ENCODE az64
	,other_cost VARCHAR(720)   ENCODE lzo
	,gl_ship_date DATE   ENCODE az64
	,"year" INTEGER   ENCODE az64
	,"quarter" VARCHAR(99)   ENCODE lzo
	,"fiscal_month" VARCHAR(99)   ENCODE lzo
	,invoice_date DATE   ENCODE az64
	--,ordered_month VARCHAR(720)   ENCODE lzo
	--,ordered_year INTEGER   ENCODE az64
	--,ordered_quarter VARCHAR(99)   ENCODE lzo
	,country VARCHAR(720)   ENCODE lzo
	,serial_number VARCHAR(720)   ENCODE lzo
	,line_type VARCHAR(720)   ENCODE lzo
	,reverser_nacells VARCHAR(720)   ENCODE lzo
	--,customer_ref_number VARCHAR(720)   ENCODE lzo
	--,aircraft_msn VARCHAR(720)   ENCODE lzo
	,aircraft_number VARCHAR(720)   ENCODE lzo
	,aircraft_delivery_date DATE   ENCODE az64
	,po_ship_date DATE   ENCODE az64
	--,contract_price VARCHAR(720)   ENCODE lzo
	--,escalation_code VARCHAR(720)   ENCODE lzo
	--,index1_base VARCHAR(720)   ENCODE lzo
	--,index2_base VARCHAR(720)   ENCODE lzo
	--,base_year VARCHAR(720)   ENCODE lzo
	--,escalation_cap_code VARCHAR(720)   ENCODE lzo
	--,unit_price NUMERIC(38,4)   ENCODE az64
	--,gta_letter VARCHAR(720)   ENCODE lzo
	,order_trx_name VARCHAR(720)   ENCODE lzo
	--,ordered_item VARCHAR(720)   ENCODE lzo
	,attribute8 VARCHAR(720)   ENCODE lzo
	,record_type VARCHAR(720)   ENCODE lzo
	,oar_customer_number VARCHAR(30)   ENCODE lzo
	,credit_cd VARCHAR(30)   ENCODE lzo	
	,qty NUMERIC(38,4)   ENCODE az64
	,audit_insert_dt TIMESTAMP WITH TIME ZONE   ENCODE az64
	,audit_update_dt TIMESTAMP WITH TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
; 



-- Permissions

GRANT ALL ON TABLE ods_git.nr_cp_shipped_open_order_stg1 TO group ods_owners;

truncate table ods_git.nr_cp_shipped_open_order_stg1;
insert into ods_git.nr_cp_shipped_open_order_stg1 (record_type,line_number,order_number,invoice_number,serial_number,"year","Quarter",fiscal_month,
	gl_ship_date,invoice_date,line_type,install_spare,reverser_nacells,engine_model,aircraft_number ,
	ultimate_customer,oar_customer_number,country,credit_cd,product_type,engine_family,sub_family,
	department,qty,sales,concessions,concessions_discount,net_sales,shop_cost,rsp_sales,rsp_concessions,
	other_cost,aircraft_delivery_date,po_ship_date,attribute8,order_trx_name,concessions_ap_misc,audit_insert_dt)
select distinct record_type,line_number,order_number,invoice_number,serial_number,ship_year,ship_quarter,ship_month,
	ship_date,invoice_date,line_type,install_spare,reverser_nacells,engine_model,aircraft_number ,
	ultimate_customer,oar_customer_number,country,credit_cd,product_type,engine_family,sub_family,
	department,qty,sales,concessions,concessions_discount,net_sales,shop_cost,rsp_sales,rsp_concessions,
	other_cost,aircraft_delivery_date,po_ship_date,attribute8,order_trx_name,concessions_ap_misc,getdate()
from ods_git.nr_shipped_open_order_stg where product_type NOT IN ('C16','C17');
				vstatus := 'Step 68 complete';	
				
				call ods_git.sp_nr_procedure_run_log('sp_nr_cp_shipped_open_order_stg1',vstatus,'nr_cp_shipped_open_order_stg1',2,'','','F');

return_val = 1;

EXCEPTION
  WHEN OTHERS THEN
 --   RAISE
 -- EXCEPTION
  raise INFO 'ERROR IN EXECUTION shipped_open_order_union1-%',SQLERRM;
 
 call ods_git.sp_nr_procedure_run_log('sp_nr_cp_shipped_open_order_stg1',vstatus,'nr_cp_shipped_open_order_stg1',3,substring(sqlerrm,1,99),vstatus,'F');
   
return_val = 0;
END;






$$
;



