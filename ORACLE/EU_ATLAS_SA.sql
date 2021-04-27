 SELECT *
      FROM
        (WITH OORDDTA AS
        (SELECT CAST (INTGRT_AFF_CD AS NUMBER(38)) AS OPER_AFF_ID,
          ORD_NO,
          INTGRT_AFF_CD,
          CAST (INTGRT_AFF_CD AS NUMBER(38)) AS OPER_CNTRY_ID,
          CAST (
          CASE
            WHEN SHP_CNTRY='ZA'
            THEN 570
            WHEN SHP_CNTRY='NA'
            THEN 740
            WHEN SHP_CNTRY='BW'
            THEN 750
            ELSE 570
          END AS NUMBER(38))                                                                                         AS SHP_CNTRY_ID,
          CAST(from_tz(CAST(ORD_DT AS TIMESTAMP),'America/Detroit') at TIME zone 'Africa/Johannesburg' AS TIMESTAMP) AS ORD_DT,
          CAST(
          CASE
            WHEN ORD_TYPE = 'ZA_COMBINED_ORDER'
            THEN 'true'
            ELSE 'false'
          END AS VARCHAR2(5)) AS COMB_ORD_FLAG,
          CAST(COALESCE (MAX (
          CASE
            WHEN SHP_IMC_NO = VOL_IMC_NO
            AND SHP_IMC_NO  = ORD_IMC_NO
            AND ORD_TYPE    = 'ZA_COMBINED_ORDER'
            THEN ORD_NO
            ELSE NULL
          END) OVER (PARTITION BY TO_NUMBER(TO_CHAR (ORD_DT,'YYYYMMDD')), ORD_TYPE, SHP_IMC_NO, SHP_ADR_LN_1), ORD_NO) AS NUMBER(38))         AS COMB_ORD_ID,
          CAST(ORD_NO AS                                                                                                  NUMBER(38))         AS ORD_ID,
          CAST(CURCY_CD AS                                                                                                VARCHAR2(3 CHAR))   AS CURCY_CD,
          CAST(SALE_CHNL AS                                                                                               VARCHAR2(240 CHAR)) AS ORD_CHANNEL,
          CAST(SHP_POSTL_CD AS                                                                                            VARCHAR2(240 CHAR)) AS SHP_POSTAL_CD,
          CAST ((
          CASE
            WHEN CANCELLED_FLG='N'
            THEN 'false'
            WHEN CANCELLED_FLG='Y'
            THEN 'true'
            ELSE 'false'
          END) AS VARCHAR2(5)) AS ORD_CANC_FLAG,
          CAST ((
          CASE
            WHEN OPEN_FLG='N'
            THEN 'true'
            WHEN OPEN_FLG='Y'
            THEN 'false'
            ELSE 'true'
          END) AS           VARCHAR2(5))        AS ORD_SHP_FLAG,
          CAST (ORD_TYPE AS VARCHAR2(240 CHAR)) AS ORD_TYPE_CD
        FROM DWSATM01.DWT42000_ORD_HDR_ATLAS
        WHERE INTGRT_AFF_CD = '570'
          --AND OORDDTA.SHP_IMC_NO in ('10002','10003','10007','10009')
          --AND OORDDTA.ORD_NO = 1003989152 --1004154793
          --AND OORDDTA.VOL_IMC_NO <> OORDDTA.ORD_IMC_NO
          --and ORD_BUS_NATR_CD = 1
        AND TO_NUMBER(TO_CHAR (CAST(from_tz(CAST(ORD_DT AS TIMESTAMP),'America/Detroit') at TIME zone 'Africa/Johannesburg' AS TIMESTAMP),'YYYYMM'))=YEARMONTH
        ),
        -----OORDLIN
        OORDLIN AS
        (SELECT ORD_NO,
          INTGRT_AFF_CD,
          CAST(WHSE_CD AS VARCHAR2(240 CHAR))                                                                            AS WHSE_CD,
          CAST(from_tz(CAST(ACT_SHP_DT AS TIMESTAMP),'America/Detroit') at TIME zone 'Africa/Johannesburg' AS TIMESTAMP) AS SHP_DT,
          CASE
            WHEN SUBSTR(VOL_IMC_NO, 1, 3) = '570'
            THEN CAST (SUBSTR(VOL_IMC_NO, 4) AS NUMBER(38))
            ELSE CAST (VOL_IMC_NO AS            NUMBER(38))
          END AS VOL_ACCOUNT_ID,
          CASE
            WHEN SUBSTR(ORD_IMC_NO, 1, 3) = '570'
            THEN CAST (SUBSTR(ORD_IMC_NO, 4) AS NUMBER(38))
            ELSE CAST (ORD_IMC_NO AS            NUMBER(38))
          END AS ORD_ACCOUNT_ID,
          CASE
            WHEN SUBSTR(SHP_IMC_NO, 1, 3) = '570'
            THEN CAST (SUBSTR(SHP_IMC_NO, 4) AS NUMBER(38))
            ELSE CAST (SHP_IMC_NO AS            NUMBER(38))
          END                        AS SHP_ACCOUNT_ID,
          CAST (DLVRY_FEE AS NUMBER) AS DELIVERY_FEE_NET,
          ORD_LN_NO,
          ORACLE_LINE_ID,
          CAST (
          CASE
            WHEN ITEM_DISP_CD = ' '
            THEN '*'
            ELSE ITEM_DISP_CD
          END AS VARCHAR2(240 CHAR)) AS ORD_LN_DISP_CD,
          TRIM(COALESCE(MAX (
          CASE
            WHEN ITEM_TYPE = 'MODEL'
            THEN ORD_ITEM
            ELSE NULL
          END) OVER (PARTITION BY ORD_NO, ORD_LN_NO), ORD_ITEM)) AS ORD_ITEM_CD,
          TRIM(ORD_ITEM)                                         AS SHP_ITEM_CD,
          CAST (UNIT_SELL_PRICE * ORD_QTY as NUMBER) as ADJ_LN_LC_NET,
          CAST (EXTEND_PV_AMT AS    NUMBER)                      AS ADJ_LN_PV,
          CAST (EXTEND_BV_AMT AS    NUMBER)                      AS ADJ_LN_BV,
          CAST (
          CASE
            WHEN ITEM_TYPE <> 'OPTION'
            THEN ORD_QTY
            ELSE 0
          END AS NUMBER) AS ORD_QTY,
          CAST (
          CASE
            WHEN ITEM_TYPE <> 'MODEL'
            THEN SHP_QTY
            ELSE 0
          END AS                NUMBER)             AS SHP_QTY,
          CAST (SHP_PRIORITY AS VARCHAR2(240 CHAR)) AS DELIVERY_MODE_CD
        FROM DWSATM01.DWT42001_ORD_LN_ATLAS
        WHERE INTGRT_AFF_CD='570'
        )
      -----Main
      SELECT OORDDTA.OPER_AFF_ID,
        OORDDTA.OPER_CNTRY_ID,
        OORDDTA.SHP_CNTRY_ID,
        OORDDTA.ORD_DT,
        CAST(TO_CHAR (OORDDTA.ORD_DT,'YYYYMMDD') AS NUMBER(8)) AS ORD_DT_KEY_NO,
        CAST(TO_CHAR (OORDDTA.ORD_DT,'YYYYMM') AS   NUMBER(6)) AS ORD_MO_YR_ID,
        OORDLIN.SHP_DT,
        CAST(TO_CHAR (OORDLIN.SHP_DT,'YYYYMMDD') AS NUMBER(8)) AS SHP_DT_KEY_NO,
        CAST(TO_CHAR (OORDLIN.SHP_DT,'YYYYMM') AS   NUMBER(6)) AS SHP_MO_YR_ID,
        OORDDTA.COMB_ORD_FLAG,
        OORDDTA.COMB_ORD_ID,
        OORDDTA.ORD_ID,
        OORDDTA.ORD_ID AS INV_CD, -- инвойс номер равен заказу (согласно MRMS)
        OORDDTA.CURCY_CD,
        OORDDTA.ORD_CHANNEL,
        OORDLIN.WHSE_CD,
        OORDLIN.WHSE_CD AS ORD_WHSE_CD,
        OORDDTA.SHP_POSTAL_CD,
        CASE
          WHEN MRMS_INVOICES.ORD_BUS_NATR_CD IN (3, 6)
          THEN OORDLIN.ORD_ACCOUNT_ID
          WHEN OORDLIN.ORD_ACCOUNT_ID       <> OORDLIN.VOL_ACCOUNT_ID
          AND MRMS_INVOICES.ORD_BUS_NATR_CD <> 3
          THEN OORDLIN.VOL_ACCOUNT_ID
          WHEN OORDLIN.ORD_ACCOUNT_ID                    = OORDLIN.VOL_ACCOUNT_ID
          AND COALESCE(MRMS_INVOICES.ORD_BUS_NATR_CD, 1) = 1
          THEN OORDLIN.VOL_ACCOUNT_ID
          ELSE OORDLIN.VOL_ACCOUNT_ID
        END AS ACCOUNT_ID,
        CAST (
        CASE
          WHEN MRMS_INVOICES.ORD_BUS_NATR_CD = 1
          THEN 'ABO'
          WHEN MRMS_INVOICES.ORD_BUS_NATR_CD = 3
          THEN 'Member'
          WHEN MRMS_INVOICES.ORD_BUS_NATR_CD = 6
          THEN 'Employee'
          ELSE 'ABO'
        END AS VARCHAR2(8)) AS ACCOUNT_TYPE_ORD,
        --MRMS_INVOICES.ORD_BUS_NATR_CD, --для теста
        OORDLIN.VOL_ACCOUNT_ID,
        OORDLIN.ORD_ACCOUNT_ID,
        OORDLIN.SHP_ACCOUNT_ID,
        OORDDTA.ORD_CANC_FLAG,
        OORDDTA.ORD_SHP_FLAG,
        CAST ('Y' AS VARCHAR2(5)) PAY_REQ_FLAG, -- недоступно --поле отсутствует в Атомик (есть в самом Atlas по словам СА менеджеров)
        CAST (NULL AS   VARCHAR2(5)) AS ORD_PAID_FLAG,
        CAST (NULL AS   NUMBER)      AS ORG_ORD_ID, -- взять из MRMS
        OORDDTA.ORD_TYPE_CD,
        CAST (NULL AS VARCHAR2(240 CHAR)) AS DROP_CD,
        OORDLIN.DELIVERY_FEE_NET,
        CAST (RANK () OVER (PARTITION BY OORDDTA.ORD_NO ORDER BY OORDLIN.ORD_LN_NO ASC, ORACLE_LINE_ID ASC) AS NUMBER(38)) ORD_LN_ID,
        CAST (OORDLIN.ORD_LN_NO AS                                                                             NUMBER(38)) AS REF_LN_ID,
        OORDLIN.ORD_LN_DISP_CD,
        CAST(OORDLIN.ORD_ITEM_CD AS VARCHAR2(240 CHAR)) AS ORD_ITEM_CD,
        CAST(OORDLIN.SHP_ITEM_CD AS VARCHAR2(240 CHAR)) AS SHP_ITEM_CD,
        CAST (
        CASE
          WHEN INSTR(regexp_replace(SUBSTR(OORDLIN.ORD_ITEM_CD, 4, LENGTH(OORDLIN.ORD_ITEM_CD)                               -3), '[B-Z]', 'A'), 'A') > 0
          THEN SUBSTR(OORDLIN.ORD_ITEM_CD, 1, INSTR(regexp_replace(SUBSTR(OORDLIN.ORD_ITEM_CD, 4, LENGTH(OORDLIN.ORD_ITEM_CD)-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE OORDLIN.ORD_ITEM_CD
        END AS VARCHAR2(240 CHAR)) AS ORD_ITEM_BASE_CD,
        CAST (
        CASE
          WHEN INSTR(regexp_replace(SUBSTR(OORDLIN.SHP_ITEM_CD, 4, LENGTH(OORDLIN.SHP_ITEM_CD)                               -3), '[B-Z]', 'A'), 'A') > 0
          THEN SUBSTR(OORDLIN.SHP_ITEM_CD, 1, INSTR(regexp_replace(SUBSTR(OORDLIN.SHP_ITEM_CD, 4, LENGTH(OORDLIN.SHP_ITEM_CD)-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE OORDLIN.SHP_ITEM_CD
        END AS VARCHAR2(240 CHAR)) AS SHP_ITEM_BASE_CD,
        OORDLIN.ADJ_LN_LC_NET,
        OORDLIN.ADJ_LN_PV,
        OORDLIN.ADJ_LN_BV,
        OORDLIN.ORD_QTY,
        OORDLIN.SHP_QTY,
        OORDLIN.DELIVERY_MODE_CD,
        CAST (NULL AS   VARCHAR2(50 CHAR)) AS CONSULTANT_CD,
        CAST(SYSDATE AS TIMESTAMP(6))      AS UPDATE_DT
      FROM OORDLIN OORDLIN
      INNER JOIN OORDDTA OORDDTA
      ON OORDDTA.INTGRT_AFF_CD = OORDLIN.INTGRT_AFF_CD
      AND OORDDTA.ORD_NO       = OORDLIN.ORD_NO
      LEFT JOIN
        (SELECT INVOICE_CD,
          AFF_ID,
          VOL_ACCOUNT_ID,
          CAST(ORD_BUS_NATR_CD AS NUMBER) AS ORD_BUS_NATR_CD
        FROM DWSEAI01.MRMS_INVOICES
        WHERE AFF_ID    =570
        AND ORD_MO_YR_ID=YEARMONTH
        ) MRMS_INVOICES
      ON OORDDTA.ORD_NO         =MRMS_INVOICES.INVOICE_CD
      AND OORDDTA.OPER_AFF_ID   =MRMS_INVOICES.AFF_ID
      AND OORDLIN.VOL_ACCOUNT_ID=MRMS_INVOICES.VOL_ACCOUNT_ID
        )
