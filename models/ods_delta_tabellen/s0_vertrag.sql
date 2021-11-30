{{ config( materialized='table',
           post_hook = "UPDATE INM_DATASHARING.AD_VERARB_ODS SET endezeit = CURRENT_TIMESTAMP, STATUS = 'done' WHERE ODS_LAUF_ID  = (SELECT max(ods_lauf_id) FROM INM_DATASHARING.AD_VERARB_ODS)" ) }}  

with source_data as (

 SELECT
    (SELECT max(ods_lauf_id) FROM INM_DATASHARING.AD_VERARB_ODS)                                 AS LOAD_NR,
    VK.TIK_VERTRAG                        AS VERTRAG_ID,
    VK.VERTRAGTYP_ID                      AS VERTRAGSART_ID,
    VK.PRODUKTPARTNER_ID                  AS PRODUKTPARTNER_ID,
    cast(NVL(
        (
            CASE
                WHEN VSV.STATUSVERTRAG IS NOT NULL
                     AND CVS.TIK_VERTRAG IS NULL THEN
                    'M||ZUSTAND||' || VSV.STATUSVERTRAG
                ELSE
                    NULL
            END
        ),
        (
            CASE
                WHEN VK.STATUS = 'ABGNG'     THEN
                    'M||ZUSTAND||abgegangen'
                WHEN VK.STATUS = 'KB'        THEN
                    'M||ZUSTAND||keine Bear'
                WHEN VK.STATUS = 'RUH'       THEN
                    'M||ZUSTAND||ruhend'
                WHEN VK.STATUS = 'STOR'      THEN
                    'M||ZUSTAND||storniert'
                WHEN VK.STATUS = 'ABG'       THEN
                    'M||ZUSTAND||abgelehnt'
                WHEN VK.STATUS = 'AKT'       THEN
                    'M||ZUSTAND||ausgefert.'
                WHEN VK.STATUS = 'ANGEB'     THEN
                    'M||ZUSTAND||angebahnt'
                WHEN VK.STATUS = 'ANGEBO'    THEN
                    'M||ZUSTAND||angeboten'
                WHEN VK.STATUS = 'ANTR'      THEN
                    'M||ZUSTAND||beantragt'
                WHEN VK.STATUS = 'BEIFR'     THEN
                    'M||ZUSTAND||beitragsfrei'
                WHEN VK.STATUS = 'BULEIS'    THEN
                    'M||ZUSTAND||BU-Leistungsfall'
                WHEN VK.STATUS = 'LP'        THEN
                    'M||ZUSTAND||leistungspflichtig'
                ELSE
                    NULL
            END
        )
    )  as varchar(255) )                                   AS STATUS_ID,
    COALESCE(
        VSV.SPARTE_REPORTING,
        VK.SPARTE_ID
    )                                     AS SPARTE_ID,
    (
        CASE
            WHEN VK.EXTERNEROB IS NOT NULL THEN
                VK.EXTERNEROB
            ELSE
                SOR.EXTERNEGRAUEOBS(
                    VK.VERTRAGOB,
                    'V'
                )
        END
    )                                     AS OB_GRAU,
    EXV2.OB                               AS VSNR_EXTERN,
    (
        CASE
            WHEN ( VSV.VERTRAGSTDOB IS NOT NULL
                   AND VSV.VERTRAGSTDOB NOT LIKE '%||%' ) THEN
                SOR.EXTERNEGRAUEOBS(
                    VSV.VERTRAGSTDOB,
                    'V'
                )
            WHEN IR5.OB IS NOT NULL THEN
                SOR.EXTERNEGRAUEOBS(
                    LTRIM(
                        IR5.OB,
                        '0'
                    ),
                    'V'
                )  --ir5.ob ohne führende Nullen
            ELSE
                NULL
        END
    )                                     AS POLICENNR,
    (
        CASE
            WHEN VSVS.RAHMENVERTRAGNR_LEBEN IS NULL THEN
                VSVS.RAHMENVERTRAGNRREF
            ELSE
                VSVS.RAHMENVERTRAGNR_LEBEN
        END
    )                                     AS SAMMELNR_RVNR,
    cast(null AS varchar(255))            AS RAHMENVERTRAG_NAME,
    IROB.OB                               AS BUENDELNR,
    NVL(
        VSV.BEGINNVERTRAG,
        VK.VERTRAGSBEGINN
    )                                     AS BEGINN,
    CASE
        WHEN VSV.VERTRAGSTDOB LIKE '%||TVP' THEN
            NULL
        ELSE
            NVL(
                VSV.ENDEVERTRAG,
                VK.VERTRAGSABLAUF
            )
    END                                   AS ABLAUF,
    VSB.NETTOJAHRESBEITRAG                AS JAHRESNETTOBEITRAG,
    VSB.BRUTTOJAHRESBEITRAG               AS JAHRESBRUTTOBEITRAG,
    VSB.BRUTTOBEITRAGZAHLWEISE            AS BRUTTOBEITRAG_GEM_ZW,
    CASE
        WHEN VSVS.ZAHLWEISE IS NULL THEN
            NULL
        ELSE
            'M||ZAHLWEISE||' || VSVS.ZAHLWEISE
    END                                   AS ZAHLWEISE_ID,
    CASE
        WHEN VSVS.ZAHLUNGSART IS NULL THEN
            NULL
        ELSE
            'M||ZAHLWEG||' || VSVS.ZAHLUNGSART
    END                                   AS ZAHLWEG_ID,
    NULL                                  AS MAHNSTAND_ID,
    DECODE(
        WGT.TIK_VERTRAG,
        NULL,
        '0',
        '1'
    )                                     AS WGT,
    VK.SEGMENT                            AS VERKAUFSPRODUKT,
    CASE
        WHEN VSV.EXTERNERPRODUKTNAME IS NOT NULL
             OR VSV.PRODUKTNAME IS NOT NULL THEN
            NVL(
                VSV.EXTERNERPRODUKTNAME,
                VSV.PRODUKTNAME
            )
        ELSE
            VK.PRODUKT
    END                                   AS PRODUKT,
    CASE
        WHEN VSV.AV_SCHICHT IS NULL THEN
            NULL
        ELSE
            CONCAT(
                'S||AV_SCHICHT_E4||',
                VSV.AV_SCHICHT
            )
    END                                   AS SCHICHT_ID,
    CASE
        WHEN VSV.BAV_FINANZIERUNG IS NULL THEN
            NULL
        ELSE
            CONCAT(
                'S||BAV_DURCHFUEHRUNG||',
                VSV.BAV_DURCHFUEHRUNG
            )
    END                                   AS DURCHFUEHRUNGSWEG_ID,
    CASE
        WHEN VSV.BAV_FINANZIERUNG = 'DVAN' THEN
            'S||BAV_FINANZIERUNG||AN'
        WHEN VSV.BAV_FINANZIERUNG IS NULL THEN
            NULL
        ELSE
            CONCAT(
                'S||BAV_FINANZIERUNG||',
                VSV.BAV_FINANZIERUNG
            )
    END                                   AS FINANZIERUNGSBEDARF_ID,
    NVL(
        VSV.AENDERUNGSDATUM,
        NVL(
            VSVS.ERSTELLTAM,
            VK.BEARBEITUNGSDATUM
        )
    )                                     AS LETZTE_AENDERUNG,
    P.DWH_PARTNER_ID                      AS BEARBEITUNG_PARTNER_ID,
    NULL                                  AS MANDANTNR,
    NULL                                  AS HV_ORGANR,
    1                                     AS IS_ACTIVE,
    VK.TIK_VERTRAG                        AS ODS_PK1,
    IR.OB                                 AS OB_BLAU,
    CASE
        WHEN IR2.OB IS NOT NULL THEN
            SUBSTR(
                IR2.OB,
                0,
                8
            )
            || '-'
            || SUBSTR(
                IR2.OB,
                9,
                5
            )
        ELSE
            NULL
    END                                   AS OB_GRUEN,
    CASE
        WHEN VSV.VERTRAGSART_LEBEN IS NULL THEN
            NULL
        ELSE
            'M||VERTRAGSART_LEBEN||' || VSV.VERTRAGSART_LEBEN
    END                                   AS VERTRAGSART_LEBEN_ID,
    CASE
        WHEN SUBSTR(
                VC3.KURZBEZEICHNUNG,
                1,
                1
            ) = 'H'
             AND IR2.OB IS NOT NULL THEN
            SUBSTR(
                IR2.OB,
                0,
                8
            )
            || '-'
            || SUBSTR(
                IR2.OB,
                9,
                5
            ) --HDI
            WHEN SUBSTR(
                VC3.KURZBEZEICHNUNG,
                1,
                1
            ) = 'G'
             AND IR.OB IS NOT NULL THEN
            IR.OB --GERL
            WHEN SUBSTR(
            VC3.KURZBEZEICHNUNG,
            1,
            1
        ) = 'B' THEN
            (
                CASE
                    WHEN VK.EXTERNEROB IS NOT NULL THEN
                        VK.EXTERNEROB
                    ELSE
                        sor.EXTERNEGRAUEOBS(
                            VK.VERTRAGOB,
                            'V'
                        )
                END
            ) --BST oder BIN
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG IN ( 14,
                                              81 )
             AND EXV.OB IS NOT NULL THEN
            EXV.OB --ROL und Koop Rest
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG = 21
             AND IR.OB IS NOT NULL THEN
            IR.OB --HLV
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG = 22
             AND IR.OB IS NOT NULL THEN
            IR.OB --HPK
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG = 23
             AND IR.OB IS NOT NULL THEN
            IR.OB --HPF
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG = 31
             AND IR.OB IS NOT NULL THEN
            IR.OB --Ampega
            WHEN VK.VERTRAGSTRAEGERZUORDNUNG = 11
             AND IR2.OB IS NOT NULL THEN
            SUBSTR(
                IR2.OB,
                0,
                8
            )
            || '-'
            || SUBSTR(
                IR2.OB,
                9,
                5
            ) --Global SE
            ELSE
            SOR.EXTERNEGRAUEOBS(
                VK.VERTRAGOB,
                'V'
            )
    END                                   AS VSNR,
    NULL                                  AS SERVICE_TARIF_ID,
    VSV.STORNODATUMZEIT                   AS STORNODATUM,
    CASE
        WHEN VSV.STORNOGRUND IS NOT NULL
             AND VSV.STORNOGRUND NOT IN ( 'Kein Stornogrund',
                                          '0' )
             AND CVS.TIK_VERTRAG IS NULL THEN
            'M||STORNOGRUND||' || VSV.STORNOGRUND
        WHEN CVS.TIK_VERTRAG IS NOT NULL
             AND CVS.STORNOGRUND IS NOT NULL THEN
            'M||STORNOGRUND||' || CVS.STORNOGRUND
        WHEN VSV.STORNODATUMZEIT IS NOT NULL
             AND VSV.STATUSVERTRAG = 'storniert'
             AND VSV.STORNOGRUND IS NULL
             AND CVS.STORNOGRUND IS NULL THEN
            'M||STORNOGRUND||929MAN'
        ELSE
            NULL
    END                                   AS STORNOGRUND_ID,
    IR4.TIK_VERTRAG                       AS VORVERTRAG_ID,
    NULL                                  AS URSACHE_VORVERTRAG_ID,
    NULL                                  AS FOLGEVERTRAG_ID,
    NULL                                  AS URSACHE_FOLGEVERTRAG_ID,
    TO_NUMBER(
        VSV.HATSELFSERVICE
    )                                     AS SELFSERVICE_TARIF,
  --vsv.ANTRAGSDATUM AS ANTRAGSDATUM, --Alte Logik bis 20.08.2019
      CASE
        WHEN MONTHS_BETWEEN(
            VSV.ANTRAGSDATUM,
            NVL(
                VSV.BEGINNVERTRAG,
                VK.VERTRAGSBEGINN
            )
        ) > 6
             OR VSV.ANTRAGSDATUM IS NULL THEN
            NVL(
                VSV.BEGINNVERTRAG,
                VK.VERTRAGSBEGINN
            )
        ELSE
            VSV.ANTRAGSDATUM
    END                                   AS ANTRAGSDATUM,
    CASE
        WHEN VSV.ZUGANGSART IS NOT NULL THEN
            'M||ZUGANGSART||' || VSV.ZUGANGSART
        ELSE
            NULL
    END                                   AS ZUGANGSART_ID,
    VSV.MATERIELLERBEGINN,
    VSV.GENERATIONENDATUM,
    CASE
        WHEN VSV.TARIFVARIANTE IS NOT NULL THEN
            'M||TARIFVARIANTE||' || VSV.TARIFVARIANTE
        ELSE
            NULL
    END                                   AS TARIFVARIANTE_ID,
    VSV.VERSICHERUNGSSUMME,
    CASE
        WHEN VSV.SUMMENART IS NOT NULL
             AND VSV.SUMMENART <> 'Unspezifizierte Vers'
             AND VSV.SUMMENART <> 'Unspezifizierte Versicherungssumme' THEN
            'M||ART_SUMME||' || VSV.SUMMENART
        ELSE
            NULL
    END                                   AS DECKUNGSSUMME_ART_ID,
    CASE
        WHEN VSV.UNTERVERSICHERUNGSVERZICHT IS NOT NULL THEN
            'M||UNTERVERS_VERZICHT||' || VSV.UNTERVERSICHERUNGSVERZICHT
        ELSE
            NULL
    END                                   AS UV_VERZICHT_ID,
    CASE
        WHEN VSV.SANIERUNGSKENNZEICHEN IS NOT NULL THEN
            'M||SANIERUNGSKENNZEICHEN||' || VSV.SANIERUNGSKENNZEICHEN
        ELSE
            NULL
    END                                   AS SANIERUNG_KZ_ID,
    NULL                                  AS PREISFINDUNGSART_ID,
    VSVS.ANZVERTRAGBUENDELNACHLASS        AS ANZ_VERTRAEGE_BUENDEL_NL,
    VSV.KEINERHOEHUNGBUENDELNACHLSSIND    AS KEINE_ERHOEHUNG_BZ,
    VSV.KEINBUENDELNACHLASSIND            AS KEIN_BUENDELNACHLASS,
    VSV.KEINFAMILIENNACHLASSIND           AS KEIN_FAMILIEN_NACHLASS,
    VSV.ANPASSUNGSSATZ                    AS ANPASSUNGSSATZ,
    VSV.ABWEICHANPASSUNGSSATZ             AS ABW_ANPASSUNGSSATZ,
    NULL                                  AS STAT_VERSICHERUNGSSUMME,
    VSV.MINDESTBEITRAG                    AS MINDESTBEITRAG,
    VSV.ISTPFLICHTVERSICHERUNGIND         AS PFLICHTVERSICHERUNG,
    VSB.STEUERNABGABENKUMUL               AS STEUERN_ABGABEN_KUMUL,
    PAKQUISE.DWH_PARTNER_ID               AS VERTRIEB_PARTNER_ID,
    PSCHADEN.DWH_PARTNER_ID               AS SCHADEN_PARTNER_ID,
    PINKASSO.DWH_PARTNER_ID               AS INKASSO_PARTNER_ID,
    NULL                                  AS VOLLMACHSTUFE_ID,
    VSV.GENERATIONENERMITTLUNGSDATUM      AS GEN_ERMITTEL_DATUM,
    NULL                                  AS VERTRIEBSWEG_ID,
    VV.VERTRIEBSZUGANGSWEG_ID             AS VERTRIEBSZUGANGSWEG_ID,
    NULL                                  AS ANPASSUNGSSATZ_KUMULIERT,
    VSB.NETTOBEITRAGZAHLWEISE             AS NETTOBEITRAG_GEM_ZW,
    VSB.TARIFBEITRAG                      AS TARIFBEITRAG_VERS_SUMME,
    VSB.JAHRESSUMMEZUSCHLAGNACHLASS       AS ZUNA_JAHRESSUMME,
    NULL                                  AS MINDESTBETRAG_MASCH,
    NULL                                  AS STEUERLAND_ID,
    VSV.ISTSTEUERINLAENDERIND             AS STEUERPFLICHTIGES_LAND,
    CASE
        WHEN VSV.STEUERBEFREIUNG IS NULL THEN
            NULL
        ELSE
            'M||BEFREIUNG_UMFANG||' || VSV.STEUERBEFREIUNG
    END                                   AS STEUERBEFREIUNG_ID,
    VV.VAMOSORGANR                        AS ORGANR,
    NULL                                  AS LETZTES_MAHNDATUM,
    CASE
        WHEN VSV.HATDRITTRECHTIND IS NULL THEN
            NULL
        ELSE
            CONCAT(
                'S||KENNZEICHEN||',
                VSV.HATDRITTRECHTIND
            )
    END                                   AS DRITTRECHT_ID,
    VSV.VERSICHERUNGSSUMME                AS VERS_SUMME_GES,
    STW.ABLGEWINN                         AS ABLAUF_GEWINN,
    STW.ABLSUMME                          AS ABLAUF_SUMME,
    STW.ERLGEWINN                         AS ERL_GEWINN,
    STW.ERLSUMME                          AS ERL_SUMME,
    STW.RUECKGEWINN                       AS RUECK_GEWINN,
    STW.RUECKKWERT                        AS RUECKK_WERT,
    STW.DT_STICHTAG                       AS STICHTAG,
    VSV.DYNAMIK_ERHOEHUNG                 AS DYNAMIK_ERHOEHUNG,
    CASE
        WHEN VSV.DYNAMIKART IS NOT NULL THEN
            'S||DYNAMIKART_LV||' || DYNAMIKART
        ELSE
            NULL
    END                                   AS DYNAMIKART_ID,
    VSV.DYNAMIKSATZ                       AS DYNAMIKSATZ,
    VSV.GARANTIESUMME                     AS GARANTIESUMME,
    CASE
        WHEN VSV.VERSORGUNGSSCHICHT IS NOT NULL THEN
            'S||VERSORGUNGSSCHICHT||' || VERSORGUNGSSCHICHT
        ELSE
            NULL
    END                                   AS VERSORGUNGSSCHICHT_ID,
    CASE
        WHEN VSV.HATBESTANDSMANAGMENTIND = 'J'     THEN
            1
        WHEN VSV.HATBESTANDSMANAGMENTIND = 'N '    THEN
            0
        ELSE
            NULL
    END                                   AS BESTANDSMANAGEMENT,
    NVL(
        BEZ.BEZUGSRECHT,
        0
    )                                     AS BEZUGSRECHT,
    VSV.WIRKSAMKEITSDATUM                 AS WIRKSAMKEITSBEGINN,
    VK.BESTANDSFUEHRUNG_ID                AS BESTANDSFUEHRUNG_ID,
    VK.INKASSOART_ID                      AS INKASSOART_ID,
    VK.BESTANDSSCHLUESSEL                 AS BESTANDSSCHLUESSEL,
    VSV.VU_NUMMER                         AS VU_NUMMER,
    CASE
        WHEN VSV.VERTRAGSFORM IS NOT NULL THEN
            CONCAT(
                'M||VERTRAGSFORM||',
                VSV.VERTRAGSFORM
            )
        ELSE
            NULL
    END                                   AS VERTRAGSFORM_ID,
    CASE
        WHEN VSV.AENDERUNGSGRUND IS NOT NULL THEN
            CONCAT(
                'M||AENDERUNGSGRUND||',
                VSV.AENDERUNGSGRUND
            )
        ELSE
            NULL
    END                                   AS AENDERUNGSGRUND_ID,
    NULL                                  AS IS_POLICE
FROM
         da.OD_VERTRAGSKONTEXT_AKT VK
    INNER JOIN {{ref('ad_mod_vertrag_coc')}}            AD ON VK.TIK_VERTRAG = AD.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_VERTRAG_AKT                   VSV ON VK.TIK_VERTRAG = VSV.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_VERTRAGSTANDARD_AKT VSVS ON VSV.TIK_POLICE = VSVS.TIK_POLICE
    LEFT OUTER JOIN da.OD_EXTERNEREFERENZ_AKT ER ON VK.TIK_VERTRAG = ER.TIK_VERTRAG
                                              AND ER.TYP IN ( 'ATR VNr',
                                                              'DKV VNr',
                                                              'Roland Vers. Nr.',
                                                              'IDEAL-Vers-Nr.' )
    LEFT OUTER JOIN da.OD_BEITRAG_AKT VSB ON VSV.VERTRAGSOB = VSB.VERTRAGSOB
                                          AND VSB.VERTRAGSTEILBUENDELID IS NULL
                                          AND VSB.VERTRAGSTEILID = '0'
    LEFT OUTER JOIN (
        SELECT
            TIK_VERTRAG
        FROM
            da.OD_WERTUNGSGRUNDLTEILUNG_AKT 
        GROUP BY
            TIK_VERTRAG
    )                                WGT ON VK.TIK_VERTRAG = WGT.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_PARTNER_AKT P ON VK.BEARBEITUNG = P.OB
                                     AND P.PARTNERART = 'OE'
    LEFT OUTER JOIN da.OD_INTERNEREFERENZ_AKT IR ON VK.TIK_VERTRAG = IR.TIK_VERTRAG
                                              AND IR.TYP = 'GEP XV'
    LEFT OUTER JOIN da.OD_INTERNEREFERENZ_AKT              IR2 ON VK.TIK_VERTRAG = IR2.TIK_VERTRAG
                                               AND IR2.TYP = 'HDI V'
    LEFT OUTER JOIN da.OD_CODE_AKT VC ON VK.ZWEIG_ID = VC.DWH_CODE_ID
    LEFT OUTER JOIN da.OD_CODE_AKT VC2 ON VC2.SCHEMA = 'VERTRAGSARTLEBEN'
                                      AND VC2.SCHLUESSEL = VSV.VERTRAGSART_LEBEN
    LEFT JOIN da.OD_CODE_AKT                        VC3 ON VK.BESTANDSFUEHRUNG_ID = VC3.DWH_CODE_ID
                                AND VC3.SCHEMA = 'BESTANDSFUEHRUNG'
    LEFT JOIN da.OD_EXTERNEREFERENZ_AKT EXV ON VK.TIK_VERTRAG = EXV.TIK_VERTRAG
                                         AND EXV.TYP IN ( 'ROL',
                                                          'IDE',
                                                          'DKV',
                                                          'ATR' )
    LEFT JOIN da.OD_WERTUNGSGRUNDLTEILUNG_AKT     WGT2 ON VK.TIK_VERTRAG = WGT2.TIK_VERTRAG
                                                    AND VK.VERMITTLERVEREINBARUNGOB = WGT2.VERMITTLERVEREINBARUNGOB
    LEFT JOIN da.OD_CODE_AKT                        VC4 ON VC4.SCHEMA = 'STORNOGRUND'
                                AND VC4.SCHLUESSEL = VSV.STORNOGRUND
    LEFT JOIN da.OD_INTERNEREFERENZ_AKT              IR3 ON VSV.TIK_POLICE = IR3.TIK_VERTRAG
                                         AND IR3.TYP = 'VVN'
    LEFT JOIN da.OD_INTERNEREFERENZ_AKT              IR4 ON IR3.OB = IR4.OB
                                         AND IR4.TYP = 'HDI V'
    INNER JOIN {{ref('filter_vertrag_crm')}} FILT ON VK.TIK_VERTRAG = FILT.TIK_VERTRAG
    LEFT JOIN (
        SELECT
            EXV.OB,
            EXV.TIK_VERTRAG
        FROM
            (
                SELECT
                    OB,
                    TIK_VERTRAG,
                    GUELTIGVON,
                    ROW_NUMBER()
                    OVER(PARTITION BY TIK_VERTRAG
                         ORDER BY GUELTIGVON DESC
                    ) AS SORTER
                FROM
                    da.OD_EXTERNEREFERENZ_AKT EXV
                WHERE
                    TYP IN ( 'ROL',
                             'IDE',
                             'DKV',
                             'ATR' )
            ) EXV
        WHERE
            EXV.SORTER = 1
    )                                EXV2 ON EXV2.TIK_VERTRAG = VK.TIK_VERTRAG
    LEFT JOIN {{ref('ad_chng_vertrag_status')}}      CVS ON CVS.TIK_VERTRAG = VK.TIK_VERTRAG
    LEFT JOIN (
        SELECT
            OB,
            TIK_VERTRAG,
            ROW_NUMBER()
            OVER(PARTITION BY OB
                 ORDER BY GUELTIGVON DESC
            ) AS SORTER
        FROM
            da.OD_INTERNEREFERENZ_AKT
        WHERE
            TYP = 'HDI B'
    )                                IROB ON IROB.TIK_VERTRAG = VK.TIK_VERTRAG
              AND IROB.SORTER = 1
    LEFT JOIN da.OD_PARTNER_AKT PAKQUISE ON VK.AKQUISE = PAKQUISE.OB
    LEFT JOIN da.OD_PARTNER_AKT                      PSCHADEN ON VK.SCHADEN = PSCHADEN.OB
    LEFT JOIN da.OD_PARTNER_AKT                      PINKASSO ON VK.INKASSO = PINKASSO.OB
    LEFT JOIN da.OD_VERMITTLERVEREINBARUNG_AKT VV ON VK.DWH_VERMVEREINBARUNG_ID = VV.DWH_VERMVEREINBARUNG_ID
    LEFT OUTER JOIN (
        SELECT
            TIK_VERTRAG,
            STW.ABLGEWINN,
            STW.ABLSUMME,
            STW.ERLGEWINN,
            STW.ERLSUMME,
            STW.RUECKGEWINN,
            STW.RUECKKWERT,
            MAX(STW.DT_STICHTAG) AS DT_STICHTAG
        FROM
            da.OD_LV_STICHTAGSWERTSATZ_AKT STW
        GROUP BY
            TIK_VERTRAG,
            STW.ABLGEWINN,
            STW.ABLSUMME,
            STW.ERLGEWINN,
            STW.ERLSUMME,
            STW.RUECKGEWINN,
            STW.RUECKKWERT
    )                                STW ON VK.TIK_VERTRAG = STW.TIK_VERTRAG
    LEFT JOIN da.OD_INTERNEREFERENZ_AKT              IR5 ON VK.TIK_VERTRAG = IR5.TIK_VERTRAG
                                         AND IR5.TYP = 'SAP'
    LEFT OUTER JOIN (
        SELECT
            TIK_VERTRAG,
            1 AS BEZUGSRECHT
        FROM
            da.OD_LV_BEZUGSRECHT_AKT 
        GROUP BY
            TIK_VERTRAG
    )                                BEZ ON VK.TIK_VERTRAG = BEZ.TIK_VERTRAG
UNION ALL 
SELECT
    (SELECT max(ods_lauf_id) FROM INM_DATASHARING.AD_VERARB_ODS)                             AS LOAD_NR,
    VK.TIK_VERTRAG                    AS VERTRAG_ID,
    VK.VERTRAGTYP_ID                  AS VERTRAGSART_ID,
    VK.PRODUKTPARTNER_ID              AS PRODUKTPARTNER_ID,
    cast( (
        CASE
            WHEN VK.STATUS = 'ABGNG'     THEN
                'M||ZUSTAND||abgegangen'
            WHEN VK.STATUS = 'KB'        THEN
                'M||ZUSTAND||keine Bear'
            WHEN VK.STATUS = 'RUH'       THEN
                'M||ZUSTAND||ruhend'
            WHEN VK.STATUS = 'STOR'      THEN
                'M||ZUSTAND||storniert'
            WHEN VK.STATUS = 'ABG'       THEN
                'M||ZUSTAND||abgelehnt'
            WHEN VK.STATUS = 'AKT'       THEN
                'M||ZUSTAND||ausgefert.'
            WHEN VK.STATUS = 'ANGEB'     THEN
                'M||ZUSTAND||angebahnt'
            WHEN VK.STATUS = 'ANGEBO'    THEN
                'M||ZUSTAND||angeboten'
            WHEN VK.STATUS = 'ANTR'      THEN
                'M||ZUSTAND||beantragt'
            WHEN VK.STATUS = 'BEIFR'     THEN
                'M||ZUSTAND||beitragsfrei'
            WHEN VK.STATUS = 'BULEIS'    THEN
                'M||ZUSTAND||BU-Leistungsfall'
            WHEN VK.STATUS = 'LP'        THEN
                'M||ZUSTAND||leistungspflichtig'
            ELSE
                NULL
        END
    )   as varchar(255) )                                AS STATUS_ID,
    VK.SPARTE_ID                      AS SPARTE_ID,
    (
        CASE
            WHEN VK.EXTERNEROB IS NOT NULL THEN
                VK.EXTERNEROB
            ELSE
                sor.EXTERNEGRAUEOBS(
                    VK.VERTRAGOB,
                    'V'
                )
        END
    )                                 AS OB_GRAU,
    NULL                              AS VSNR_EXTERN,
    NULL                              AS POLICENNR,
    VSVS.RAHMENVERTRAGNRREF           AS SAMMELNR_RVNR,
    NULL                              AS RAHMENVERTRAG_NAME,
    NULL                              AS BUENDELNR,
    VK.VERTRAGSBEGINN                 AS BEGINN,
    VK.VERTRAGSABLAUF                 AS ABLAUF,
    NULL                              AS JAHRESNETTOBEITRAG,
    NULL                              AS JAHRESBRUTTOBEITRAG,
    NULL                              AS BRUTTOBEITRAG_GEM_ZW,
    CASE
        WHEN VSVS.ZAHLWEISE IS NULL THEN
            NULL
        ELSE
            'M||ZAHLWEISE||' || VSVS.ZAHLWEISE
    END                               AS ZAHLWEISE_ID,
    CASE
        WHEN VSVS.ZAHLUNGSART IS NULL THEN
            NULL
        ELSE
            'M||ZAHLWEG||' || VSVS.ZAHLUNGSART
    END                               AS ZAHLWEG_ID,
    NULL                              AS MAHNSTAND_ID,
    DECODE(
        WGT.TIK_VERTRAG,
        NULL,
        '0',
        '1'
    )                                 AS WGT,
    VK.SEGMENT                        AS VERKAUFSPRODUKT,
    VK.PRODUKT                        AS PRODUKT,
    NULL                              AS SCHICHT_ID,
    NULL                              AS DURCHFUEHRUNGSWEG_ID,
    NULL                              AS FINANZIERUNGSBEDARF_ID,
    NVL(
        VSVS.ERSTELLTAM,
        VK.BEARBEITUNGSDATUM
    )                                 AS LETZTE_AENDERUNG,
    P.DWH_PARTNER_ID                  AS BEARBEITUNG_PARTNER_ID,
    NULL                              AS MANDANTNR,
    NULL                              AS HV_ORGANR,
    1                                 AS IS_ACTIVE,
    VK.TIK_VERTRAG                    AS ODS_PK1,
    IR.OB                             AS OB_BLAU,
    NULL                              AS OB_GRUEN,
    NULL                              AS VERTRAGSART_LEBEN_ID,
    (
        CASE
            WHEN VK.EXTERNEROB IS NOT NULL THEN
                VK.EXTERNEROB
            ELSE
                sor.EXTERNEGRAUEOBS(
                    VK.VERTRAGOB,
                    'V'
                )
        END
    )                                 AS VSNR,
    NULL                              AS SERVICE_TARIF_ID,
    NULL                              AS STORNODATUM,
    NULL                              AS STORNOGRUND_ID,
    IR4.TIK_VERTRAG                   AS VORVERTRAG_ID,
    NULL                              AS URSACHE_VORVERTRAG_ID,
    NULL                              AS FOLGEVERTRAG_ID,
    NULL                              AS URSACHE_FOLGEVERTRAG_ID,
    NULL                              AS SELFSERVICE_TARIF,
    NULL                              AS ANTRAGSDATUM,
    NULL                              AS ZUGANGSART_ID,
    NULL                              AS MATERIELLERBEGINN,
    NULL                              AS GENERATIONENDATUM,
    NULL                              AS TARIFVARIANTE_ID,
    NULL                              AS VERSICHERUNGSSUMME,
    NULL                              AS DECKUNGSSUMME_ART_ID,
    NULL                              AS UV_VERZICHT_ID,
    NULL                              AS SANIERUNG_KZ_ID,
    NULL                              AS PREISFINDUNGSART_ID,
    VSVS.ANZVERTRAGBUENDELNACHLASS    AS ANZ_VERTRAEGE_BUENDEL_NL,
    NULL                              AS KEINE_ERHOEHUNG_BZ,
    NULL                              AS KEIN_BUENDELNACHLASS,
    NULL                              AS KEIN_FAMILIEN_NACHLASS,
    NULL                              AS ANPASSUNGSSATZ,
    NULL                              AS ABW_ANPASSUNGSSATZ,
    NULL                              AS STAT_VERSICHERUNGSSUMME,
    NULL                              AS MINDESTBEITRAG,
    NULL                              AS PFLICHTVERSICHERUNG,
    NULL                              AS STEUERN_ABGABEN_KUMUL,
    PAKQUISE.DWH_PARTNER_ID           AS VERTRIEB_PARTNER_ID,
    PSCHADEN.DWH_PARTNER_ID           AS SCHADEN_PARTNER_ID,
    PINKASSO.DWH_PARTNER_ID           AS INKASSO_PARTNER_ID,
    NULL                              AS VOLLMACHSTUFE_ID,
    NULL                              AS GEN_ERMITTEL_DATUM,
    NULL                              AS VERTRIEBSWEG_ID,
    VV.VERTRIEBSZUGANGSWEG_ID         AS VERTRIEBSZUGANGSWEG_ID,
    NULL                              AS ANPASSUNGSSATZ_KUMULIERT,
    NULL                              AS NETTOBEITRAG_GEM_ZW,
    NULL                              AS TARIFBEITRAG_VERS_SUMME,
    NULL                              AS ZUNA_JAHRESSUMME,
    NULL                              AS MINDESTBETRAG_MASCH,
    NULL                              AS STEUERLAND_ID,
    NULL                              AS STEUERPFLICHTIGES_LAND,
    NULL                              AS STEUERBEFREIUNG_ID,
    VV.VAMOSORGANR                    AS ORGANR,
    NULL                              AS LETZTES_MAHNDATUM,
    NULL                              AS DRITTRECHT_ID,
    NULL                              AS VERS_SUMME_GES,
    NULL                              AS ABLAUF_GEWINN,
    NULL                              AS ABLAUF_SUMME,
    NULL                              AS ERL_GEWINN,
    NULL                              AS ERL_SUMME,
    NULL                              AS RUECK_GEWINN,
    NULL                              AS RUECKK_WERT,
    NULL                              AS STICHTAG,
    NULL                              AS DYNAMIK_ERHOEHUNG,
    NULL                              AS DYNAMIKART_ID,
    NULL                              AS DYNAMIKSATZ,
    NULL                              AS GARANTIESUMME,
    NULL                              AS VERSORGUNGSSCHICHT_ID,
    NULL                              AS BESTANDSMANAGEMENT,
    NULL                              AS BEZUGSRECHT,
    NULL                              AS WIRKSAMKEITSBEGINN,
    VK.BESTANDSFUEHRUNG_ID            AS BESTANDSFUEHRUNG_ID,
    VK.INKASSOART_ID                  AS INKASSOART_ID,
    VK.BESTANDSSCHLUESSEL             AS BESTANDSSCHLUESSEL,
    NULL                              AS VU_NUMMER,
    NULL                              AS VERTRAGSFORM_ID,
    NULL                              AS AENDERUNGSGRUND_ID,
    1                                 AS IS_POLICE
FROM
         da.OD_VERTRAGSKONTEXT_AKT VK
    INNER JOIN {{ref('ad_mod_vertrag_coc')}}         AD ON VK.TIK_VERTRAG = AD.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_VERTRAGSTANDARD_AKT        VSVS ON VK.TIK_VERTRAG = VSVS.TIK_POLICE
    LEFT OUTER JOIN (
        SELECT
            TIK_VERTRAG
        FROM
            da.OD_WERTUNGSGRUNDLTEILUNG_AKT 
        GROUP BY
            TIK_VERTRAG
    )                             WGT ON VK.TIK_VERTRAG = WGT.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_PARTNER_AKT                   P ON VK.BEARBEITUNG = P.OB
                                     AND P.PARTNERART = 'OE'
    LEFT OUTER JOIN da.OD_INTERNEREFERENZ_AKT           IR ON VK.TIK_VERTRAG = IR.TIK_VERTRAG
                                              AND IR.TYP = 'GEP XV'
    LEFT JOIN da.OD_INTERNEREFERENZ_AKT           IR3 ON VK.TIK_VERTRAG = IR3.TIK_VERTRAG
                                         AND IR3.TYP = 'VVN'
    LEFT JOIN da.OD_INTERNEREFERENZ_AKT           IR4 ON IR3.OB = IR4.OB
                                         AND IR4.TYP = 'HDI V'
    LEFT JOIN da.OD_PARTNER_AKT                   PAKQUISE ON VK.AKQUISE = PAKQUISE.OB
    LEFT JOIN da.OD_PARTNER_AKT                   PSCHADEN ON VK.SCHADEN = PSCHADEN.OB
    LEFT JOIN da.OD_PARTNER_AKT                   PINKASSO ON VK.INKASSO = PINKASSO.OB
    LEFT JOIN da.od_vermittlervereinbarung_akt    VV ON VK.DWH_VERMVEREINBARUNG_ID = VV.DWH_VERMVEREINBARUNG_ID
    LEFT OUTER JOIN da.od_vertrag_akt                VSV ON VSV.TIK_VERTRAG = VK.TIK_VERTRAG
    LEFT OUTER JOIN da.od_vertrag_akt                VSV2 ON VSV2.TIK_POLICE = VK.TIK_VERTRAG
    LEFT OUTER JOIN da.OD_INTERNEREFERENZ_AKT            IR5 ON IR5.TYP = 'SAP'
                                               AND LTRIM(
        VK.VERTRAGOB,
        '0'
    ) = LTRIM(
        IR5.OB,
        '0'
    )
    LEFT OUTER JOIN da.OD_INTERNEREFERENZ_AKT            IR6 ON IR6.TYP = 'SAP'
                                               AND IR6.TIK_VERTRAG = VK.TIK_VERTRAG
WHERE
        VK.VERTRAGTYP = 18
    AND VK.BESTANDSFUEHRUNG = '28'
    AND IR5.TIK_VERTRAG IS NULL --der Police sind keine Verträge zugeordnet
    AND IR6.TIK_VERTRAG IS NULL --kein Vertrag mit Policenzuordnung
    AND VSV.ID IS NULL --Kein Vertrag aus dem Bestandssystem 
    AND VSV2.ID IS NULL -- der Police werden keine Verträge aus dem Bestandssystem zugeordnet
    AND VK.VERTRAGOB <> '000000000'
    AND VK.BESTANDSSCHLUESSEL NOT IN ( '22',
                                       '25',
                                       '44' )
    AND ( VK.STATUS NOT IN ( 'ABG',
                             'ABGNG',
                             'ANGEB',
                             'ANGEBO',
                             'ANTR',
                             'KB',
                             'RUH',
                             'STOR' )
          OR VK.STATUS IS NULL
         --OR NVL(VK.STATUSDATUM,current_timestamp) > dateadd(year, -10, to_date(current_timestamp))
          OR VK.BESTANDSSCHLUESSEL <> '80' )
    AND ( VK.STATUS NOT IN ( 'ABG',
                             'ABGNG',
                             'ANGEB',
                             'ANGEBO',
                             'ANTR',
                             'KB',
                             'RUH',
                             'STOR' )
          OR VK.STATUS IS NULL
       --  OR NVL(VK.STATUSDATUM,current_timestamp) > dateadd(year, -10, to_date(current_timestamp)) 
        
          OR VK.BESTANDSSCHLUESSEL = '80' )


					)

select *
from source_data