-- =======================================================
-- 1. STORED PROCEDURE FOR LOGGING PIPELINE CHECKPOINTS
-- =======================================================
DROP PROCEDURE IF EXISTS UpdateWatermarkTable;
GO

CREATE PROCEDURE UpdateWatermarkTable
    @lastload VARCHAR(200)
AS
BEGIN
    BEGIN TRANSACTION;

    UPDATE water_table
    SET last_load = @lastload;

    COMMIT TRANSACTION;
END;
GO

-- =======================================================
-- 2. PIPELINE DEBUGGING, TESTING & RESET UTILITIES
-- =======================================================

-- Active Data Monitoring
SELECT * FROM source_cars_data;
SELECT * FROM water_table;
SELECT COUNT(*) AS TotalRows FROM source_cars_data;

-- Environment Reset Commands (For Re-running Initial vs Incremental Loads)
TRUNCATE TABLE source_cars_data;

UPDATE water_table
SET last_load = 'DT00000';

-- Post-Reset Verification
SELECT COUNT(*) AS TotalRows FROM source_cars_data;
SELECT * FROM water_table;