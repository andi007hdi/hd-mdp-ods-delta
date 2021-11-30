

        insert into DWHHD_DEV.INM_DATASHARING.ad_verarb_ods ("ODS_LAUF_ID", "STARTZEIT", "ENDEZEIT", "LAUF_DAT", "STATUS")
        (
            select "ODS_LAUF_ID", "STARTZEIT", "ENDEZEIT", "LAUF_DAT", "STATUS"
            from DWHHD_DEV.INM_DATASHARING.ad_verarb_ods__dbt_tmp
        );