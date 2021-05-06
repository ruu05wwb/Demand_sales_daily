BEGIN
  INSERT INTO DWSEAI01.DS_PENDING_MONTHS
  SELECT T0.*, 0 AS UPDT_ON_LAST_RUN_FLG
  FROM
    (SELECT DISTINCT 'EU_AS400'   AS SRC,
      CAST(ORD_DT/100 AS INTEGER) AS ORD_MO_YR_ID
    FROM DWSATM01.DWT42016_ORD_HDR_OMS
    WHERE COALESCE(INTGRT_CTL_AFF, CTL_AFF)                  IN ('150', '480', '040')
    AND ORD_DT  >=20210201
    AND TO_CHAR(LAST_UPDT_DT_KEY_NO, 'YYYYMMDD')              =TO_CHAR(SYSDATE, 'YYYYMMDD')
    UNION ALL
    SELECT DISTINCT 'EU_HYBR_NON_EOM'                           AS SRC,
      EXTRACT(YEAR FROM ORD_DT) *100+EXTRACT(MONTH FROM ORD_DT) AS ORD_MO_YR_ID
    FROM DWSATM01.DWT42130_ORD_HDR_HYB T0
    WHERE NVL(INTGRT_AFF_CD, AFF_CD)                                 IN ('160', '090', '470', '250', '430', '420', '060', '110', '210')
    AND EXTRACT(YEAR FROM ORD_DT)*100+EXTRACT(MONTH FROM ORD_DT)>=202102
    AND TO_CHAR(UPDT_DT, 'YYYYMMDD')                                  =TO_CHAR(SYSDATE, 'YYYYMMDD')
    UNION ALL
    SELECT DISTINCT 'EU_HYBR_EOM'                                                AS SRC,
      EXTRACT(YEAR FROM ORDER_DATE_TIME)*100+EXTRACT(MONTH FROM ORDER_DATE_TIME) AS ORD_MO_YR_ID
    FROM DWSATM01.DWT42231_ORD_HDR_EAP
    WHERE AFFILIATE_CODE IN ('030'
      /*, '100', '350', '500'*/
      )
    AND EXTRACT(YEAR FROM ORDER_DATE_TIME)*100+EXTRACT(MONTH FROM ORDER_DATE_TIME)>=202102
    AND TO_CHAR(UPDT_DT, 'YYYYMMDD')                                               =TO_CHAR(SYSDATE, 'YYYYMMDD')
    UNION ALL
    SELECT DISTINCT 'EU_ATLAS'                                                                                                                         AS SRC,
      CAST(TO_CHAR (CAST(from_tz(CAST(ORD_DT AS TIMESTAMP),'America/Detroit') at TIME zone 'Africa/Johannesburg' AS TIMESTAMP),'YYYYMM') AS NUMBER(6)) AS ORD_MO_YR_ID
    FROM DWSATM01.DWT42000_ORD_HDR_ATLAS
    WHERE INTGRT_AFF_CD                                                                                                                                  = '570'
    AND CAST(TO_CHAR (CAST(from_tz(CAST(ORD_DT AS TIMESTAMP),'America/Detroit') at TIME zone 'Africa/Johannesburg' AS TIMESTAMP),'YYYYMM') AS NUMBER(6))>=202102
    AND TO_CHAR(UPDT_DT, 'YYYYMMDD')                                                                                                                     =TO_CHAR(SYSDATE, 'YYYYMMDD')
    ) T0
  WHERE NOT EXISTS
    (SELECT SRC,
      ORD_MO_YR_ID
    FROM DWSEAI01.DS_PENDING_MONTHS DS_PENDING_MONTHS
    WHERE T0.SRC       =DS_PENDING_MONTHS.SRC
    AND T0.ORD_MO_YR_ID=DS_PENDING_MONTHS.ORD_MO_YR_ID
    );
 UPDATE DWSEAI01.DS_PENDING_MONTHS SET UPDT_ON_LAST_RUN_FLG=UPDT_ON_LAST_RUN_FLG+1 WHERE UPDT_ON_LAST_RUN_FLG>0;
  COMMIT;
END;
