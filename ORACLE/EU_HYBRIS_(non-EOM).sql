      SELECT *
      FROM
        (WITH OORDDTA AS
        (SELECT COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)                                          AS oper_cntry_cd,
          CAST(COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) AS INTEGER)                               AS oper_cntry_id,
          CAST(COALESCE(INTGRT_AFF_CD, AFF_CD) AS           INTEGER)                               AS oper_aff_id,
          CAST(DLVRY_ADDR_AMWAY_CNTRY_CD AS                 INTEGER)                               AS shp_cntry_id,
          ORD_DT                                                                                   AS ord_dt,
          EXTRACT(YEAR FROM ORD_DT) *10000+EXTRACT(MONTH FROM ORD_DT)*100+EXTRACT(DAY FROM ORD_DT) AS ord_dt_key_no,
          EXTRACT(YEAR FROM ORD_DT) *100+EXTRACT(MONTH FROM ORD_DT)                                AS ord_mo_yr_id,
          CASE
            WHEN COMB_ORD_FLG='Y'
            THEN 'true'
            WHEN COMB_ORD_FLG='N'
            THEN 'false'
          END                            AS comb_ord_flag,
          CAST(COMB_ORD_NO AS INTEGER)   AS comb_ord_id,
          CAST(ORD_NO AS      INTEGER)   AS ord_id,
          INV_NO                         AS inv_cd,
          ORD_CURCY_CD                   AS curcy_cd,
          ORD_CHNL_CD                    AS ord_channel,
          WHSE_NM                        AS whse_cd,
          COALESCE(ORD_WHSE_NM, WHSE_NM) AS ord_whse_cd,
          TRIM(DLVRY_ADDR_POST_CD)       AS shp_postal_cd,
          CASE
            WHEN (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)='160'
            AND VOL_IMC_NO                                 ='3000001')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)  ='060'
            AND VOL_IMC_NO                                 ='8')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('210', '080', '110')
            AND VOL_IMC_NO                                 ='3')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('480')
            AND VOL_IMC_NO                                 ='5')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('620','650','390','800','340','820','810','150','590','490','140','450')
            AND VOL_IMC_NO                                 =COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD))
              /*In Eastern Europe, dummy ids are equal to country code, excepting Ukraine*/
            THEN DWSEAI01.STR2INT(RPAD(TRIM(DLVRY_ADDR_FAM_NM
              ||DLVRY_ADDR_GIVEN_NM),7,'X'))
            ELSE CAST(BILL_IMC_NO AS INTEGER)
          END AS account_id,
          CASE
            WHEN (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)='160'
            AND VOL_IMC_NO                                 ='3000001')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)  ='060'
            AND VOL_IMC_NO                                 ='8')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('210', '080', '110')
            AND VOL_IMC_NO                                 ='3')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('480')
            AND VOL_IMC_NO                                 ='5')
            OR (COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) IN ('620','650','390','800','340','820','810','150','590','490','140','450')
            AND VOL_IMC_NO                                 =COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD))
              /*In Eastern Europe, dummy ids are equal to country code, excepting Ukraine*/
            THEN 'FOA'
            ELSE
              CASE
                WHEN TRIM(BILL_IMC_TYPE_CD)='E'
                THEN 'Employee'
                WHEN TRIM(BILL_IMC_TYPE_CD) IN ('SOCIO CONSUMIDOR', 'I', 'ABO SE', 'ABO', 'AMWAY DIRECT RETAILER', 'VIP Customer')
                THEN 'ABO'
                ELSE 'Customer'
              END
          END                         AS imc_type_ord,
          CAST(VOL_IMC_NO AS INTEGER) AS vol_account_id,
          CAST(ORD_IMC_NO AS INTEGER) AS ord_account_id,
          CAST(SHP_IMC_NO AS INTEGER) AS shp_account_id,
          CASE
            WHEN ORD_CANC_FLG='Y'
            THEN 'true'
            ELSE 'false'
          END AS ord_canc_flag,
          CASE
            WHEN ORD_SHP_FLG='Y'
            THEN 'true'
            ELSE 'false'
          END         AS ord_shp_flag,
          PAY_REQ_FLG AS pay_req_flag,
          CASE
            WHEN ORD_PAY_STAT_CD='PAID'
            THEN 'true'
            ELSE 'false'
          END                         AS ord_paid_flag,
          ORGNL_ORD_NO                AS org_ord_id,
          ORD_TYPE_CD                 AS ord_type_cd,
          DROP_CD                     AS drop_cd,
          DLVRY_ACTUAL_COST_CURCY_AMT AS delivery_fee_net,
          DLVRY_MODE_CD               AS delivery_mode_cd
        FROM DWSATM01.DWT42130_ORD_HDR_HYB T0
        WHERE (NVL(INTGRT_AFF_CD, AFF_CD)                           IN ('160', '090', '470', '250', '430', '420', '060', '110', '210')
        OR (NVL(INTGRT_AFF_CD, AFF_CD)                              IN ('150', '480')
        AND YEARMONTH                                               >=202109))
        AND EXTRACT(YEAR FROM ORD_DT) *100+EXTRACT(MONTH FROM ORD_DT)=YEARMONTH
        ),
        -----OORDLIN
        OORDLIN AS
        (SELECT INTGRT_CNTRY_CD,
          CAST(COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD) AS INTEGER) AS oper_cntry_id,
          ORD_NO                                                     AS ord_id,
          CAST(ORD_LN_NO AS INTEGER)                                 AS ord_ln_id,
          CAST(REF_LN_NO AS INTEGER)                                 AS ref_ln_id,
          ITEM_DISP_DESC                                             AS ord_ln_disp_cd,
          trim(COALESCE(ORD_CD, ITEM_ID))                            AS ord_item_cd,
          trim(ITEM_ID)                                              AS shp_item_cd,
          COALESCE(CAST(ADJ_ITEM_PRICE_AMT AS FLOAT),0)              AS adj_ln_lc_net,
          CASE
            WHEN COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD)='430'
            THEN TOT_PV_AMT
            ELSE COALESCE(CAST(ADJ_ITEM_PRICE_PV_AMT AS FLOAT),ITEM_PV_AMT)
          END                                                        AS adj_ln_pv,
          COALESCE(CAST(ADJ_ITEM_PRICE_BV_AMT AS FLOAT),ITEM_BV_AMT) AS adj_ln_bv,
          CASE
            WHEN MIN(ORD_LN_NO) over (partition BY COALESCE(T0.INTGRT_CNTRY_CD, T0.CNTRY_CD), ORD_NO, COALESCE(ORD_CD, ITEM_ID))=ORD_LN_NO
            THEN CAST(COALESCE(COMPNT_QTY, ITEM_QTY_AMT) AS FLOAT)
            ELSE 0
          END                         AS ord_qty,
          CAST(ITEM_QTY_AMT AS FLOAT) AS shp_qty,
          POS_CONSLTNT_ID             AS consultant_id
        FROM DWSATM01.DWT42131_ORD_LN_HYB T0
        WHERE (COALESCE(INTGRT_AFF_CD, AFF_CD) IN ('160', '090', '470', '250', '430', '420', '060', '110', '210')
        OR (COALESCE(INTGRT_AFF_CD, AFF_CD)    IN ('150', '480')
        AND YEARMONTH                          >=202109))
        ),
        ORD_CONSIGN_HYB AS
        (SELECT INTGRT_CNTRY_CD,
          CAST(DWT42134_ORD_CONSIGN_HYB.ORD_NO AS INTEGER) AS ORD_NO,
          MAX(DWT42134_ORD_CONSIGN_HYB.SHP_DT) SHP_DT
        FROM DWSATM01.DWT42134_ORD_CONSIGN_HYB DWT42134_ORD_CONSIGN_HYB
        WHERE (INTGRT_CNTRY_CD IN ('210','080','060','160','120','090','470','460','280','370','270','250','110','430','830','420')
        OR (INTGRT_CNTRY_CD    IN ('620','650','390','800','340','820','810','150','590','490','140','450','660','480')
        AND YEARMONTH          >=202109))
        GROUP BY INTGRT_CNTRY_CD,
          CAST(DWT42134_ORD_CONSIGN_HYB.ORD_NO AS INTEGER)
        ) -----Main
      SELECT OORDDTA.oper_aff_id                                                                                                                                           AS oper_aff_id,
        OORDDTA.oper_cntry_id                                                                                                                                              AS oper_cntry_id,
        OORDDTA.shp_cntry_id                                                                                                                                               AS shp_cntry_id,
        OORDDTA.ord_dt                                                                                                                                                     AS ord_dt,
        CAST(OORDDTA.ord_dt_key_no AS NUMBER(8, 0))                                                                                                                        AS ord_dt_key_no,
        CAST(OORDDTA.ord_mo_yr_id AS  NUMBER(6, 0))                                                                                                                        AS ord_mo_yr_id,
        ORD_CONSIGN_HYB.SHP_DT                                                                                                                                             AS shp_dt,
        CAST( EXTRACT(YEAR FROM ORD_CONSIGN_HYB.SHP_DT ) *10000+EXTRACT(MONTH FROM ORD_CONSIGN_HYB.SHP_DT )*100+EXTRACT(DAY FROM ORD_CONSIGN_HYB.SHP_DT ) AS NUMBER(8, 0)) AS shp_dt_key_no,
        CAST(EXTRACT(YEAR FROM ORD_CONSIGN_HYB.SHP_DT )  *100+EXTRACT(MONTH FROM ORD_CONSIGN_HYB.SHP_DT ) AS                                                 NUMBER(6, 0)) AS shp_mo_yr_id,
        OORDDTA.comb_ord_flag                                                                                                                                              AS comb_ord_flag,
        OORDDTA.comb_ord_id                                                                                                                                                AS comb_ord_id,
        OORDDTA.ord_id                                                                                                                                                     AS ord_id,
        OORDDTA.inv_cd                                                                                                                                                     AS inv_cd,
        OORDDTA.curcy_cd                                                                                                                                                   AS curcy_cd,
        OORDDTA.ord_channel                                                                                                                                                AS ord_channel,
        OORDDTA.whse_cd                                                                                                                                                    AS whse_cd,
        OORDDTA.ord_whse_cd                                                                                                                                                AS ord_whse_cd,
        OORDDTA.shp_postal_cd                                                                                                                                              AS shp_postal_cd,
        OORDDTA.account_id                                                                                                                                                 AS account_id,
        OORDDTA.imc_type_ord                                                                                                                                               AS account_type_ord,
        OORDDTA.vol_account_id                                                                                                                                             AS vol_account_id,
        OORDDTA.ord_account_id                                                                                                                                             AS ord_account_id,
        OORDDTA.shp_account_id                                                                                                                                             AS shp_account_id,
        OORDDTA.ord_canc_flag                                                                                                                                              AS ord_canc_flag,
        OORDDTA.ord_shp_flag                                                                                                                                               AS ord_shp_flag,
        OORDDTA.pay_req_flag                                                                                                                                               AS pay_req_flag,
        OORDDTA.ord_paid_flag                                                                                                                                              AS ord_paid_flag,
        CAST(OORDDTA.org_ord_id AS NUMBER)                                                                                                                                 AS org_ord_id,
        OORDDTA.ord_type_cd                                                                                                                                                AS ord_type_cd,
        OORDDTA.drop_cd                                                                                                                                                    AS drop_cd,
        OORDDTA.delivery_fee_net                                                                                                                                           AS delivery_fee_net,
        OORDLIN.ord_ln_id                                                                                                                                                  AS ord_ln_id,
        OORDLIN.ref_ln_id                                                                                                                                                  AS ref_ln_id,
        OORDLIN.ord_ln_disp_cd,
        OORDLIN.ord_item_cd AS ord_item_cd,
        OORDLIN.shp_item_cd AS shp_item_cd,
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 5, LENGTH(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))))                                                                -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR)))
        END AS ord_item_base_cd,
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 5, LENGTH(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))))                                                                -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR)))
        END                           AS shp_item_base_cd,
        OORDLIN.adj_ln_lc_net         AS adj_ln_lc_net,
        OORDLIN.adj_ln_pv             AS adj_ln_pv,
        OORDLIN.adj_ln_bv             AS adj_ln_bv,
        OORDLIN.ord_qty               AS ord_qty,
        OORDLIN.shp_qty               AS shp_qty,
        OORDDTA.delivery_mode_cd      AS delivery_mode_cd,
        OORDLIN.consultant_id         AS consultant_cd,
        CAST(SYSDATE AS TIMESTAMP(6)) AS UPDATE_DT
      FROM OORDLIN OORDLIN
      LEFT JOIN OORDDTA OORDDTA
      ON OORDLIN.ord_id        =OORDDTA.ord_id
      AND OORDLIN.oper_cntry_id=OORDDTA.oper_cntry_id
      LEFT JOIN ORD_CONSIGN_HYB ORD_CONSIGN_HYB
      ON OORDLIN.ord_id          =ORD_CONSIGN_HYB.ORD_NO
      AND OORDLIN.INTGRT_CNTRY_CD=ORD_CONSIGN_HYB.INTGRT_CNTRY_CD
      WHERE OORDDTA.ORD_ID      IS NOT NULL
        )
