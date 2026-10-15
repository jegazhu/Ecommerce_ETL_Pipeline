USE [OlistEcommerce_Destination]
GO
/****** Object:  StoredProcedure [dbo].[sp_Sync_Databricks_Ventas]    Script Date: 10/15/2026 6:54:00 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[sp_Sync_Databricks_Ventas]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- 1. Drop temp table if it exists
        IF OBJECT_ID('tempdb..#TempRemoteVentas') IS NOT NULL
            DROP TABLE #TempRemoteVentas;

        -- 2. Create the temp table EXPLICITLY in the outer scope.
        --    SQL Server cannot resolve a temp table that is only created
        --    inside dynamic SQL, so it must be declared here.
        CREATE TABLE #TempRemoteVentas (
            anio              INT,
            mes               INT,
            ventas_totales    DECIMAL(18,2),
            numero_pedidos    INT,
            ticket_promedio   DECIMAL(18,2),
            unidades_vendidas DECIMAL(18,2)
        );

        -- 3. Pull from Databricks via OPENROWSET into the temp table
        --    PrepareMethod=0 disables the remote PREPARE round-trip that
        --    MSDASQL was choking on with the Databricks ODBC driver.
        DECLARE @sql NVARCHAR(MAX);
        SET @sql = N'
            INSERT INTO #TempRemoteVentas
                (anio, mes, ventas_totales, numero_pedidos, ticket_promedio, unidades_vendidas)
            SELECT anio, mes, ventas_totales, numero_pedidos, ticket_promedio, unidades_vendidas
            FROM OPENROWSET(
                    ''MSDASQL'',
                    ''DSN=YOURDATABRICKSDSN;PrepareMethod=0;'',
                    ''SELECT anio, mes, ventas_totales, numero_pedidos, ticket_promedio, unidades_vendidas
                      FROM default.gold_ventas_por_mes''
                 ) AS x;';
        EXEC sp_executesql @sql;

        -- 4. Clear local permanent target table
        TRUNCATE TABLE dbo.gold_ventas_por_mes;

        -- 5. Move data from temp table into the final table
        INSERT INTO dbo.gold_ventas_por_mes
            (anio, mes, ventas_totales, numero_pedidos, ticket_promedio, unidades_vendidas)
        SELECT anio, mes, ventas_totales, numero_pedidos, ticket_promedio, unidades_vendidas
        FROM #TempRemoteVentas;

        PRINT 'Extraction and sync completed successfully: Databricks -> SSMS [dbo].[gold_ventas_por_mes].';
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#TempRemoteVentas') IS NOT NULL
            DROP TABLE #TempRemoteVentas;

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE(),
                @ErrorLine     INT           = ERROR_LINE();

        RAISERROR ('Sync failed at line %d: %s', 16, 1, @ErrorLine, @ErrorMessage);
    END CATCH
END;
