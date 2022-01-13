

        insert into DWHHD_DEV.INM_DATASHARING.ad_anzahl_akt_verarbeitung ("TABELLE", "DML_FLAG", "ANZAHL", "T_AEND_DAT", "ODS_LAUF_ID")
        (
            select "TABELLE", "DML_FLAG", "ANZAHL", "T_AEND_DAT", "ODS_LAUF_ID"
            from DWHHD_DEV.INM_DATASHARING.ad_anzahl_akt_verarbeitung__dbt_tmp
        );