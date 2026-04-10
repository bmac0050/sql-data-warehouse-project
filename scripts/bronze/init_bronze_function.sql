/*
=======================================
Create Bronze Schema
=======================================

Script Purpose:
	This script creates tables in the bronze schema, dropping tables if they already exist.
 	Run this script to re-define the DDL structure of 'bronze' tables.
 */

CREATE OR REPLACE PROCEDURE bronze.init_bronze ()
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS bronze.CRM_CUST_INFO;
	CREATE TABLE bronze.crm_cust_info (
		cst_id int,
		cst_key varchar(50),
		cst_firstname varchar(50),
		cst_lastname varchar(50),
		cst_marital_status varchar(50),
		cst_gndr varchar(50),
		cst_create_date DATE 
	);
	
	DROP TABLE IF EXISTS BRONZE.CRM_PRD_INFO;
	CREATE TABLE bronze.crm_prd_info (
		prd_id int,
		prd_key varchar(50),
		prd_nm varchar(50),
		prd_cost int,
		prd_line varchar(50),
		prd_start_dt date,
		prd_end_dt date
	);
	
	DROP TABLE IF EXISTS BRONZE.CRM_SALES_DETAILS; 
	CREATE TABLE bronze.crm_sales_details (
		sls_ord_num varchar(50),
		sls_prd_key varchar(50),
		sls_cust_id int,
		sls_order_dt int,
		sls_ship_dt int,
		sls_due_dt int,
		sls_sales int,
		sls_quantity int,
		sls_price int
	);
	
	DROP TABLE IF EXISTS BRONZE.ERP_CUST_AZ12;
	CREATE TABLE bronze.erp_cust_az12 (
		cid varchar(50),
		bdate date,
		gen varchar(50)
	);
	
	DROP TABLE IF EXISTS BRONZE.ERP_LOC_A101;
	CREATE TABLE bronze.erp_loc_a101 (
		cid varchar(50),
		cntry varchar(50)
	);
	
	DROP TABLE IF EXISTS BRONZE.ERP_PX_CAT_G1V2;
	CREATE TABLE bronze.erp_px_cat_g1v2 (
		id varchar(50),
		cat varchar(50),
		subcat varchar(50),
		maintenance varchar(50)
	);
END;
$$;





