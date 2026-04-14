/*
=================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================
Script Purpose:
    This stored procedure performs the ETL process top populate the 'silver' schema tables from
    the 'bronze' schema.
Actions Performed:
    - Truncates Silver Tables
    - Inserts transformed and cleansed data from Bronze into Silver tables

Parameters:
    None.
    This stored procedure does not accept any parameters or return values.

Usage Example:
CALL silver.load_silver()
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_start      timestamp;
    v_proc_end        timestamp;
    v_step_start      timestamp;
    v_step_end        timestamp;
    v_rows            bigint;
    v_error_message   text;
    v_error_detail    text;
    v_error_hint      text;
BEGIN
    v_proc_start := clock_timestamp();

    RAISE NOTICE '======================================';
    RAISE NOTICE 'Starting silver.load_silver at %', v_proc_start;
    RAISE NOTICE '======================================';

    ------------------------------------------------------------------
    -- STEP 1: silver.crm_cust_info
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.crm_cust_info';
        RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        RAISE NOTICE '>> Loading Table: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.crm_cust_info';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    ------------------------------------------------------------------
    -- STEP 2: silver.crm_prd_info
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.crm_prd_info';
        RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        RAISE NOTICE '>> Loading Table: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(substring(prd_key, 1, 5), '-', '_') AS cat_id,
            substring(prd_key, 7, length(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            (lead(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - INTERVAL '1 day')::date AS prd_end_dt
        FROM bronze.crm_prd_info;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.crm_prd_info';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    ------------------------------------------------------------------
    -- STEP 3: silver.crm_sales_details
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.crm_sales_details';
        RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';

        TRUNCATE TABLE silver.crm_sales_details;

        RAISE NOTICE '>> Loading Table: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0 OR length(sls_order_dt::text) <> 8 THEN NULL
                ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) <> 8 THEN NULL
                ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0 OR length(sls_due_dt::text) <> 8 THEN NULL
                ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales <> sls_quantity * abs(sls_price)
                THEN sls_quantity * abs(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.crm_sales_details';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    ------------------------------------------------------------------
    -- STEP 4: silver.erp_cust_az12
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.erp_cust_az12';
        RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;

        RAISE NOTICE '>> Loading Table: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > current_date THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.erp_cust_az12';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    ------------------------------------------------------------------
    -- STEP 5: silver.erp_loc_a101
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.erp_loc_a101';
        RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        RAISE NOTICE '>> Loading Table: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            replace(cid, '-', '') AS cid,
            CASE
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.erp_loc_a101';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    ------------------------------------------------------------------
    -- STEP 6: silver.erp_px_cat_g1v2
    ------------------------------------------------------------------
    BEGIN
        v_step_start := clock_timestamp();

        RAISE NOTICE '>> --------------------------------------';
        RAISE NOTICE '>> Step: silver.erp_px_cat_g1v2';
        RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISE NOTICE '>> Loading Table: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_step_end := clock_timestamp();

        RAISE NOTICE '>> Rows loaded: %', v_rows;
        RAISE NOTICE '>> Duration: %', v_step_end - v_step_start;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail  = PG_EXCEPTION_DETAIL,
                v_error_hint    = PG_EXCEPTION_HINT;

            v_step_end := clock_timestamp();

            RAISE NOTICE '!! ERROR in step silver.erp_px_cat_g1v2';
            RAISE NOTICE '!! Message: %', v_error_message;
            RAISE NOTICE '!! Detail: %', COALESCE(v_error_detail, 'n/a');
            RAISE NOTICE '!! Hint: %', COALESCE(v_error_hint, 'n/a');
            RAISE NOTICE '!! Duration before failure: %', v_step_end - v_step_start;

            RAISE;
    END;

    v_proc_end := clock_timestamp();

    RAISE NOTICE '======================================';
    RAISE NOTICE 'Load Silver Procedure Completed!';
    RAISE NOTICE 'Total Duration: %', v_proc_end - v_proc_start;
    RAISE NOTICE 'Finished at: %', v_proc_end;
    RAISE NOTICE '======================================';

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_error_message = MESSAGE_TEXT,
            v_error_detail  = PG_EXCEPTION_DETAIL,
            v_error_hint    = PG_EXCEPTION_HINT;

        v_proc_end := clock_timestamp();

        RAISE NOTICE '======================================';
        RAISE NOTICE 'Load Silver Procedure FAILED';
        RAISE NOTICE 'Message: %', v_error_message;
        RAISE NOTICE 'Detail: %', COALESCE(v_error_detail, 'n/a');
        RAISE NOTICE 'Hint: %', COALESCE(v_error_hint, 'n/a');
        RAISE NOTICE 'Total Duration Before Failure: %', v_proc_end - v_proc_start;
        RAISE NOTICE '======================================';

        RAISE;
END;
$$;
