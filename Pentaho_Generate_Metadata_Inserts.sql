--queries to create insert statements for Pentaho metadata
--be sure to type the correct etl_pattern type for the use case you are using
--be sure to type lowercase developer name (e.g.,chris.hodge) ALL OTHER VALUES SHOULD BE CAPITALIZED
--be sure to grab all records returned in plsql tool
--target table names will match source table names.  These will need to be edited should the requestor want a different table name
-----------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------

--FOR TABLES
SELECT 'INSERT INTO APP_TABLE (SOURCE_DATABASE_ID,SOURCE_SCHEMA_NAME,SOURCE_TABLE_NAME,ETL_PATTERN_TYP,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,ACTIVE_IND,LAST_MODIFY_DT,LAST_MODIFY_BY_ID) VALUES (&DATABASE_ID,'''||OWNER||''','''||TABLE_NAME||''',''&ETL_PATTERN_TYP'',''&TARGET_SCHEMA_NAME'','''||TABLE_NAME||''',''Y'','''||TRUNC(SYSDATE)||''',''&DEVELOPER_NAME'');' 
FROM all_tables 
WHERE owner = '&SOURCE_SCHEMA';
--add table names if you do not want to ingest the entire source schema


--FOR VIEWS/SYNONYMS

SELECT 'INSERT INTO APP_TABLE (SOURCE_DATABASE_ID,SOURCE_SCHEMA_NAME,SOURCE_TABLE_NAME,ETL_PATTERN_TYP,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,ACTIVE_IND,LAST_MODIFY_DT,LAST_MODIFY_BY_ID) VALUES (&DATABASE_ID,'''||OWNER||''','''||OBJECT_NAME||''',''&ETL_PATTERN_TYP'',''&TARGET_SCHEMA_NAME'','''||OBJECT_NAME||''',''Y'','''||TRUNC(SYSDATE)||''',''&DEVELOPER_NAME'');' 
FROM ALL_OBJECTS 
WHERE owner = '&SOURCE_SCHEMA'
AND OBJECT_NAME IN (
'CS_CDLORA_MAL_HIST_DISPOS',
'CS_CDLORA_MAL_HIST_ASSIGNMENT',
'CS_CDLORA_MAL2SCRUMTEAM',
'CS_CDLORA_MAL2MAL',
'CS_CDLORA_MAL');

