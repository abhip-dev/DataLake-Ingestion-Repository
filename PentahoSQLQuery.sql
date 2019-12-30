select * from dual;
select * from app_table where LAST_MODIFY_BY_ID='kumar.abhishek';
select * from app_database where LAST_MODIFY_BY_ID='kumar.abhishek';
select * from app_database where DATABASE_ID = 641;
select * from app_database where DATABASE_ID = 3;
select * from app_database where DATABASE_NAME like '%DWBS001P%';
select * from app_database where DATABASE_NAME like '%DWBS%';
select * from app_database;
select * from app_table where SOURCE_DATABASE_ID=641;
select DATABASE_ID from app_database 
select MAX(DATABASE_ID) from app_database;
select DATABASE_ID FROM app_database ORDER BY DATABASE_ID DESC;


Insert into PROD_METADATA.APP_DATABASE
   (DATABASE_ID, DATABASE_TYP, PORT, DATABASE_NAME, USER_NAME, 
    PASSWORD_VAR, LAST_MODIFY_DT, LAST_MODIFY_BY_ID, DRIVER_CLASS)
 Values
   (641, 'Oracle', -1, 'jdbc:oracle:thin:@//pddclodm-scan:1521/DWBS01PI1', 'cdlora', 
    'Encrypted 2be98afc86aa7b181a50dbb798cc2fe83', TO_DATE('02/07/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'kumar.abhishek', 'oracle.jdbc.driver.OracleDriver'); 
    
    
 
 Insert into PROD_METADATA.APP_TABLE
   (TABLE_ID, SOURCE_DATABASE_ID, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, ETL_PATTERN_TYP, 
    TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, ACTIVE_IND, LAST_MODIFY_DT, LAST_MODIFY_BY_ID, SEQNO)
Values
   (11714, 641, 'STAGE', 'CDS_CONTACT_MEDIUM_PROFILE', 'SNAPSHOT_TYPE_1', 
    'bdalab_sa', 'STAGE_CDS_CONTACT_MEDIUM_PROFILE', 'Y', TO_DATE('02/07/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'kumar.abhishek', 1);
 
 
 Insert into PROD_METADATA.APP_TABLE
   (TABLE_ID, SOURCE_DATABASE_ID, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, ETL_PATTERN_TYP, 
    TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, ACTIVE_IND, LAST_MODIFY_DT, LAST_MODIFY_BY_ID, SEQNO)
Values
   (11715, 641, 'STAGE', 'CDS_CUSTOMER_ACCOUNT', 'SNAPSHOT_TYPE_1', 
    'bdalab_sa', 'STAGE_CDS_CUSTOMER_ACCOUNT', 'Y', TO_DATE('02/07/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'kumar.abhishek', 1);
    
    Insert into PROD_METADATA.APP_TABLE
   (TABLE_ID, SOURCE_DATABASE_ID, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, ETL_PATTERN_TYP, 
    TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, ACTIVE_IND, LAST_MODIFY_DT, LAST_MODIFY_BY_ID, SEQNO)
Values
   (11716, 641, 'STAGE', 'CDS_CONTACT_MEDIUM_PROFILE_REF', 'SNAPSHOT_TYPE_1', 
    'bdalab_sa', 'STAGE_CDS_CONTACT_MEDIUM_PROFILE_REF', 'Y', TO_DATE('02/07/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'kumar.abhishek', 1);
	
	
	
	
	
	
APP_DATABASE:-
Insert into PROD_METADATA.APP_DATABASE
   (DATABASE_ID, DATABASE_TYP, PORT, DATABASE_NAME, USER_NAME, 
    PASSWORD_VAR, DRIVER_CLASS, LAST_MODIFY_DT, LAST_MODIFY_BY_ID)
Values
   (622, 'Oracle', -1, 'jdbc:oracle:thin:@//netsecdenvm244.corp.intranet:1521/dnadenp', 'cdlems', 
    'Encrypted 2be98afc86aa7f2e49b30aa33c7c1e1ff', 'oracle.jdbc.driver.OracleDriver', TO_DATE('01/21/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'tsitsi.fitshani');
COMMIT;


APP_TABLE:-
Insert into PROD_METADATA.APP_TABLE
   (TABLE_ID, SOURCE_DATABASE_ID, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, ETL_PATTERN_TYP, 
    TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, ACTIVE_IND, LAST_MODIFY_DT, LAST_MODIFY_BY_ID, 
    SEQNO)
Values
   (3702, 622, 'DNAUSER', 'htbl_dtnotuclientctp', 'SNAPSHOT_TYPE_2', 
    'EMS_INFINERA_DNA', 'dnauser_htbl_dtnotuclientctp', 'N', TO_DATE('01/10/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'tsitsi.fitshani', 1);
COMMIT;

    
    