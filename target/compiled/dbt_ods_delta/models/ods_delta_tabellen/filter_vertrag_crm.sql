

with  __dbt__CTE__od_vertragskontext_akt as (
select *
from DWHHD_DEV.da.od_vertragskontext_akt
),source_data as (

 SELECT vk.TIK_VERTRAG
  FROM __dbt__CTE__od_vertragskontext_akt VK
  WHERE VK.BESTANDSSCHLUESSEL NOT IN ('22','25')
  AND ( VK.BESTANDSSCHLUESSEL NOT IN ('44')
  OR VK.BESTANDSFUEHRUNG           ='23' )
  AND ( VK.VERTRAGTYP             <> '18'
  OR VK.VERTRAGTYP                IS NULL
  OR vk.BESTANDSFUEHRUNG          <> '28'
  OR vk.BESTANDSFUEHRUNG          IS NULL )
  AND ( VK.VERTRAGTYP NOT         IN ('18','20')
  OR VK.VERTRAGTYP                IS NULL
  OR vk.BESTANDSFUEHRUNG          <> '29'
  OR vk.BESTANDSFUEHRUNG          IS NULL )
  AND ( VK.STATUS NOT             IN ('ABG','ABGNG','ANGEB','ANGEBO','ANTR','KB','RUH','STOR')
  OR VK.STATUS                    IS NULL
  OR NVL(VK.STATUSDATUM,current_timestamp) > dateadd(year, -10, to_date(current_timestamp))
  OR VK.BESTANDSSCHLUESSEL        <> '80' )
  AND ( VK.STATUS NOT             IN ('ABG','ABGNG','ANGEB','ANGEBO','ANTR','KB','RUH','STOR')
  OR VK.STATUS                    IS NULL
  OR NVL(VK.STATUSDATUM,current_timestamp) > dateadd(year, -10, to_date(current_timestamp))
  OR VK.BESTANDSSCHLUESSEL         = '80' )
  AND VK.VERTRAGOB                <>'000000000'

					)

select *
from source_data