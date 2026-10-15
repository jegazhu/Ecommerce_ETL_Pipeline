USE OlistEcommerce_Destination;
GO
/****** Object:  StoredProcedure [dbo].[sp_Sync_Databricks_SilverSales]    Script Date: 10/15/2026 3:54:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE dbo.sp_Sync_Databricks_SilverSales
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- 1. Drop temp table if it exists
        IF OBJECT_ID('dbo.silver_ecommerce_sales', 'U') IS NOT NULL
			DROP TABLE dbo.silver_ecommerce_sales;

        -- 2. Create the temp table EXPLICITLY in the outer scope
        --    (SQL Server cannot resolve a temp table that only exists inside dynamic SQL)
        CREATE TABLE dbo.silver_ecommerce_sales (
            order_id          VARCHAR(50),
            order_date        DATE,
            customer_id       VARCHAR(50),
            product_category  VARCHAR(100),
            region            VARCHAR(100),
            quantity          DECIMAL(18,2),
            unit_price        DECIMAL(18,2),
            discount          DECIMAL(18,2),
            payment_method    VARCHAR(50),
            delivery_days     DECIMAL(10,2),
            customer_rating   DECIMAL(3,2),
            revenue           DECIMAL(18,2)
        );

		PRINT 'dbo.silver_ecommerce_sales created successfully.';

        -- 3. Pull from Databricks via OPENROWSET.
        --    PrepareMethod=0 disables the remote PREPARE round-trip that
        --    MSDASQL was choking on with the Databricks ODBC driver.
        --    All double columns are cast to DECIMAL on the remote side to
        --    avoid ODBC type-mapping surprises at prepare time.
        INSERT INTO dbo.silver_ecommerce_sales
            (order_id, order_date, customer_id, product_category, region,
             quantity, unit_price, discount, payment_method,
             delivery_days, customer_rating, revenue)
        SELECT
             order_id, order_date, customer_id, product_category, region,
             quantity, unit_price, discount, payment_method,
             delivery_days, customer_rating, revenue
        FROM OPENROWSET(
                'MSDASQL',
                'DSN=YOURDatabricksDSN;PrepareMethod=0;',
                'SELECT
                    order_id,
                    CAST(order_date AS DATE) AS order_date,
                    customer_id,
                    product_category,
                    region,
                    CAST(quantity AS DECIMAL(18,2))  AS quantity,
                    CAST(unit_price AS DECIMAL(18,2))  AS unit_price,
                    CAST(discount AS DECIMAL(18,2))  AS discount,
                    payment_method,
                    CAST(delivery_days AS DECIMAL(10,2))  AS delivery_days,
                    CAST(customer_rating AS DECIMAL(3,2))   AS customer_rating,
                    CAST(revenue AS DECIMAL(18,2))  AS revenue
                 FROM default.silver_ecommerce_sales'
             ) AS x;

        -- 4. Clear local permanent target table
        --TRUNCATE TABLE dbo.silver_ecommerce_sales;

        -- 5. Move data from temp table into the final table with a timestamp
        INSERT INTO dbo.silver_ecommerce_sales
            (order_id, order_date, customer_id, product_category, region,
             quantity, unit_price, discount, payment_method,
             delivery_days, customer_rating, revenue)
        SELECT
             order_id, order_date, customer_id, product_category, region,
             quantity, unit_price, discount, payment_method,
             delivery_days, customer_rating, revenue
        FROM dbo.silver_ecommerce_sales;

        PRINT 'Extraction and sync completed successfully: Databricks -> SSMS [dbo].[silver_ecommerce_sales].';
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#TempRemoteSilverSales') IS NOT NULL
            DROP TABLE #TempRemoteSilverSales;

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE(),
                @ErrorLine     INT           = ERROR_LINE();

        RAISERROR ('Sync failed at line %d: %s', 16, 1, @ErrorLine, @ErrorMessage);
    END CATCH
END;
GO
