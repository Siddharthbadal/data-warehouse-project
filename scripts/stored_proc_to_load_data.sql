/*
	Stored Procedure to Insert Data into Tables

	Purpose of the script is to insert bulk data into all the tables. 
	Table is turncated before we insert the data.

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze_layer_data AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME 
	BEGIN TRY
		PRINT '--------------------------------------';
		PRINT 'Loading Bronze Layer For DatawareHouse';
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the table: Customer Info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into table: Customer Info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the table: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into table: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '-------------------------';


		SET @start_time= GETDATE();
		PRINT '>> Truncating the table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into table: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '-------------------------';

		PRINT '--------------------------------------';
		PRINT 'Loading the ERP Tables';
		PRINT '--------------------------------------';

		PRINT '>> Truncating the table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into table: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '>> Truncating the table: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into table: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '>> Truncating the table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into table: px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\data-warehouse-project\data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	END TRY
	BEGIN CATCH
		PRINT '----------------------';
		PRINT 'Eror occured during loading bronze layer';
		PRINT 'Error Mesage' + ERROR_MESSAGE();
		PRINT 'Error Mesage' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Mesage' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '-----------------------';

	END CATCH
END

-- run the procedure
EXEC bronze.load_bronze_layer_data;