SELECT *
      FROM
        (WITH ORD_HDR AS
        (SELECT ORD,
          COMB_ORD,
          ORD_CURCY,
          COMB_ORD_FLG,
          INV_NBR,
          ORD_TYPE,
          DROP_CD,
          WHSE,
          ORD_WHSE,
          ORD_DT,
          ORD_TM,
          DLVR_DT,
          COALESCE(TRIM(CAST(ORD_SRC AS VARCHAR2(240 BYTE))), '*') AS ORD_SRC,
          ORD_HNDLG_PRICE,
          ORD_PAY_STAT,
          COALESCE(ORD_TAX/NULLIF(ORD_VALUE-ORD_TAX, 0), 0)              AS ord_tax_rt,
          CAST(COALESCE(TRIM(CTL_AFF), TRIM(INTGRT_CTL_AFF)) AS INTEGER) AS oper_aff_id,
          NVL(MAX(CASE
      WHEN ORD=COMB_ORD
      THEN CAST(NVL(TRIM(CNTRY),TRIM(INTGRT_CNTRY_CD)) AS VARCHAR2(3 CHAR))
      ELSE NULL
    END) OVER(PARTITION BY NVL(TRIM(CTL_AFF),TRIM(INTGRT_CTL_AFF)) , COMB_ORD, SHP_DISTB, ORD_DT),NVL(TRIM(CNTRY),TRIM(INTGRT_CNTRY_CD)))  AS OPER_CNTRY_CD, /*Override oper country assigned to order with oper country assigned to the first suborder of combined order to avoid discrepancy between oper country and currency in Eastern Europe*/
          COALESCE(TRIM(CNTRY), TRIM(INTGRT_CNTRY_CD))                   AS init_cntry_cd,
          VOL_DISTB,
          ORD_DISTB,
          SHP_DISTB,
          SHP_ADR,
          ORD_CANC,
          ORD_SHP,
          PAY_REQ,
          ORGNL_ORD,
          CASE
            WHEN (DISTB_ORD_NBR<>'BBB'
            AND ((COMB_ORD_FLG  ='N'
            AND VOL_DISTB      <>ORD_DISTB)
            OR (COMB_ORD_FLG    ='Y'
            AND (VOL_DISTB     <>ORD_DISTB
            AND ORD_DISTB      <>SHP_DISTB))))
            OR DISTB_ORD_NBR    ='MBR'
            THEN 'Y'
            ELSE 'N'
          END AS CUST_ORD_FLG
        FROM DWSATM01.DWT42016_ORD_HDR_OMS
        WHERE COALESCE(TRIM(CTL_AFF), TRIM(INTGRT_CTL_AFF)) IN ('150', '480', '040')
        AND ORD_DT BETWEEN YEARMONTH                         *100+1 AND YEARMONTH*100+31
        ),
        OORDDTA AS
        (SELECT ORD AS ord_id,
          COMB_ORD  AS comb_ord_id,
          CASE
            WHEN ORD_CURCY ='CIP'
            THEN 'GBP'
            ELSE CAST(ORD_CURCY AS VARCHAR2(3 CHAR))
          END AS curcy_cd,
          CASE
            WHEN COMB_ORD_FLG='Y'
            THEN 'true'
            ELSE 'false'
          END                          AS comb_ord_flag,
          INV_NBR                      AS inv_cd,
          ORD_CHNL.GLBL_SALES_CHNL_CD  AS ord_channel,
          WHSE                         AS whse_cd,
          ORD_WHSE                     AS ord_whse_cd,
          CAST(ORD_DT/ 100 AS INTEGER) AS ord_mo_yr_id,
          TO_DATE(ORD_DT
          ||LPAD(ORD_TM,6,'0'), 'YYYYMMDDHH24MISS') AS ord_dt,
          ORD_DT                                    AS ord_dt_key_no,
          CAST(ORD_DT/ 100 AS INTEGER)              AS shp_mo_yr_id,
          TO_DATE(DLVR_DT, 'YYYYMMDD')              AS shp_dt,
          DLVR_DT                                   AS shp_dt_key_no,
          oper_aff_id,
          CAST(oper_cntry_cd AS INTEGER) AS oper_cntry_id,
          CAST(init_cntry_cd AS INTEGER) AS init_cntry_id,
          CAST(oper_cntry_cd AS INTEGER) AS shp_cntry_id,
          CASE
            WHEN CUST_ORD_FLG='Y'
            THEN ORD_DISTB
            ELSE VOL_DISTB
          END       AS account_id,
          VOL_DISTB AS vol_account_id,
          ORD_DISTB AS ord_account_id,
          SHP_DISTB AS shp_account_id,
          CASE
            WHEN ORD_CANC='N'
            THEN 'false'
            ELSE 'true'
          END AS ord_canc_flag,
          CASE
            WHEN ORD_SHP='N'
            THEN 'false'
            WHEN ORD_SHP='Y'
            THEN 'true'
          END     AS ord_shp_flag,
          PAY_REQ AS pay_req_flag,
          CASE
            WHEN ORD_PAY_STAT='P'
            THEN 'true'
            ELSE 'false'
          END       AS ord_paid_flag,
          ORGNL_ORD AS org_ord_id,
          ORD_TYPE  AS ord_type_cd,
          DROP_CD   AS drop_cd,
          CASE
            WHEN ORD_TYPE='EMP'
            THEN 'Employee'
            ELSE
              CASE
                WHEN CUST_ORD_FLG='Y'
                THEN 'Member'
                ELSE 'ABO'
              END
          END               AS imc_type_ord,
          DDSTADR.postal_cd AS shp_postal_cd,
          ORD_HNDLG_PRICE   AS delivery_fee_net
        FROM ORD_HDR ORD_HDR
        LEFT JOIN DWSATM01.dwt42051_ord_sales_chnl ORD_CHNL
        ON CAST(ORD_HDR.init_cntry_cd AS INTEGER)=CAST(ORD_CHNL.CNTRY_CD AS INTEGER)
        AND ORD_HDR.ORD_TYPE    =ORD_CHNL.ORD_TYPE_CD
        AND ORD_HDR.ORD_SRC     =ORD_CHNL.ORD_SRC_CD
        LEFT JOIN
          (SELECT CAST(COALESCE(TRIM(INTGRT_AFF_CD), TRIM(CTL_AFF)) AS INTEGER) AS aff_id,
            DISTB_NBR,
            ADR_SEQ,
            TRIM(POST) AS postal_cd
          FROM DWSATM01.DWT41052_DISTB_ADR
          WHERE CAST(COALESCE(TRIM(INTGRT_AFF_CD), TRIM(CTL_AFF)) AS INTEGER) IN (150, 480, 40)
          ) DDSTADR
        ON ORD_HDR.SHP_DISTB   =DDSTADR.DISTB_NBR
        AND ORD_HDR.oper_aff_id=DDSTADR.aff_id
        AND ORD_HDR.SHP_ADR    =DDSTADR.ADR_SEQ
        ),
        ------OORDLIN
        LINES_MAIN AS
        (SELECT ORD_LINES.ORD,
          ORD_LN,
          LN_TYPE,
          REF_LN,
          ITEM_TYPE,
          ITEM_DISP,
          ITEM,
          ITEM_QTY,
          ITEM_PRICE,
          ITEM_PV,
          ITEM_BV,
          CAST(COALESCE(TRIM(ORD_LINES.CNTRY), TRIM(ORD_LINES.INTGRT_CNTRY_CD)) AS INTEGER) AS init_cntry_id
        FROM DWSATM01.DWT42017_ORD_LN_OMS ORD_LINES
        INNER JOIN ORD_HDR ORD_HDR
        ON ORD_LINES.ORD                                                    =ORD_HDR.ORD
        AND COALESCE(TRIM(ORD_LINES.CNTRY), TRIM(ORD_LINES.INTGRT_CNTRY_CD))=ORD_HDR.init_cntry_cd
        ),
        OORDLIN AS
        (SELECT LINES_MAIN.ORD AS ord_id,
          LINES_MAIN.init_cntry_id,
          LINES_MAIN.ORD_LN                                    AS ord_ln_id,
          LINES_MAIN.REF_LN                                    AS ref_ln_id,
          LINES_MAIN.ITEM_DISP                                 AS ord_ln_disp_cd,
          TRIM(COALESCE(LINES_MAIN_ORD.ITEM, LINES_MAIN.ITEM)) AS ord_item_cd,
          TRIM(LINES_MAIN.ITEM)                                AS shp_item_cd,
          LINES_MAIN.ITEM_PRICE                                AS adj_ln_lc_net,
          LINES_MAIN.ITEM_PV                                   AS adj_ln_pv,
          LINES_MAIN.ITEM_BV                                   AS adj_ln_bv,
          CASE
            WHEN LINES_MAIN.LN_TYPE IN ('O', 'A', 'R')
            THEN LINES_MAIN.ITEM_QTY
            ELSE 0
          END AS ord_qty,
          CASE
            WHEN LINES_MAIN.ITEM_TYPE <>'I'
              /*Set shp_qty to 0 if: 1. It is not a product (e.g. promo)*/
            OR LINES_MAIN.ITEM_DISP IN ('S')
              /*2. It is an item which was ordered but then replaced with another item*/
            OR (LINES_MAIN.ORD_LN <>LINES_MAIN.REF_LN
              /*3. It is a bundle component or SKU shipped automatically together with main SKU...*/
            AND LINES_MAIN.ITEM_TYPE <>'I')
              /*... but only if it is a product; This logic complies with GDW, but in case if some bundle component is replaced, it returns qty>0 for both non-shipped component, and replacement. On the other hand, if applying "LINES_MAIN_STAT.ORD_LN_USED_PROD_CNT>0" instead of "LINES_MAIN.ITEM_TYPE<>'I'" then shipped qty's will become correct (0 for replaced item and >0 for replacement, but as replacement has ORD_ITEM equal to the replaced item not to bundle, in this case, the logic of bundle would be broken*/
            OR (LINES_MAIN.ORD_LN =LINES_MAIN.REF_LN
              /*4. First item in a series of items sharing the same ORD_ITEM...*/
            AND (LINES_MAIN_STAT.ORD_LN_USED_PROD_CNT >0
              /*which is either a bundle of products (non-promo)*/
            OR COALESCE(LINES_MAIN_STAT.ORD_LN_USED_CNT, 0)=0)
              /*or replaced item - but only if this is an independent item, i.e. not a bundle component*/
            AND LINES_MAIN.ITEM_DISP IN ('E', 'S'))
              /*E for bundle, S for replaced item*/
            THEN 0
            ELSE LINES_MAIN.ITEM_QTY
          END AS shp_qty,
          LINES_MAIN.ITEM_QTY
        FROM LINES_MAIN LINES_MAIN
        LEFT JOIN LINES_MAIN LINES_MAIN_ORD
        ON LINES_MAIN.ORD           =LINES_MAIN_ORD.ORD
        AND LINES_MAIN.REF_LN       =LINES_MAIN_ORD.ORD_LN
        AND LINES_MAIN.init_cntry_id=LINES_MAIN_ORD.init_cntry_id
        LEFT JOIN
          (SELECT init_cntry_id,
            ORD,
            REF_LN,
            COUNT(
            CASE
              WHEN LN_TYPE='P'
              THEN NULL
              ELSE ORD_LN
            END) AS ORD_LN_USED_PROD_CNT,
            /*Count of non-promo items within the same ordered item, excepting the main one*/
            COUNT(ORD_LN) AS ORD_LN_USED_CNT
            /*Count of all items within the same ordered item, excepting the main one*/
          FROM LINES_MAIN
          WHERE ORD_LN<>REF_LN
            /*Don't consider the main item within group (e.g. virtual SKU in a bundle*/
          GROUP BY init_cntry_id,
            ORD,
            REF_LN
          ) LINES_MAIN_STAT
        ON LINES_MAIN.ORD           =LINES_MAIN_STAT.ORD
        AND LINES_MAIN.ORD_LN       =LINES_MAIN_STAT.REF_LN
        AND LINES_MAIN.init_cntry_id=LINES_MAIN_STAT.init_cntry_id
        )
      -----Main
      SELECT OORDDTA.oper_aff_id                        AS oper_aff_id,
        OORDDTA.oper_cntry_id                           AS oper_cntry_id,
        OORDDTA.shp_cntry_id                            AS shp_cntry_id,
        CAST(OORDDTA.ord_dt AS        TIMESTAMP(6))     AS ord_dt,
        CAST(OORDDTA.ord_dt_key_no AS NUMBER(8, 0))     AS ord_dt_key_no,
        CAST(OORDDTA.ord_mo_yr_id AS  NUMBER(6, 0))     AS ord_mo_yr_id,
        CAST(OORDDTA.shp_dt AS        TIMESTAMP(6))     AS shp_dt,
        CAST(OORDDTA.shp_dt_key_no AS NUMBER(8, 0))     AS shp_dt_key_no,
        CAST(OORDDTA.shp_mo_yr_id AS  NUMBER(6, 0))     AS shp_mo_yr_id,
        OORDDTA.comb_ord_flag                           AS comb_ord_flag,
        CAST(OORDDTA.comb_ord_id AS   NUMBER(38, 0))      AS comb_ord_id,
        CAST(OORDDTA.ord_id AS        NUMBER(38, 0))      AS ord_id,
        CAST(OORDDTA.inv_cd AS        VARCHAR2(240 CHAR)) AS inv_cd,
        CAST(OORDDTA.curcy_cd AS      VARCHAR2(3 CHAR))   AS curcy_cd,
        CAST(OORDDTA.ord_channel AS   VARCHAR2(240 CHAR)) AS ord_channel,
        CAST(OORDDTA.whse_cd AS       VARCHAR2(240 CHAR)) AS whse_cd,
        CAST(OORDDTA.ord_whse_cd AS   VARCHAR2(240 CHAR)) AS ord_whse_cd,
        CAST(OORDDTA.shp_postal_cd AS VARCHAR2(240 CHAR)) AS shp_postal_cd,
        CAST(OORDDTA.account_id AS    NUMBER)             AS account_id,
        OORDDTA.imc_type_ord                              AS account_type_ord,
        CAST(OORDDTA.vol_account_id AS NUMBER(38, 0))     AS vol_account_id,
        CAST(OORDDTA.ord_account_id AS NUMBER(38, 0))     AS ord_account_id,
        CAST(OORDDTA.shp_account_id AS NUMBER(38, 0))     AS shp_account_id,
        OORDDTA.ord_canc_flag                             AS ord_canc_flag,
        OORDDTA.ord_shp_flag                              AS ord_shp_flag,
        OORDDTA.pay_req_flag                              AS pay_req_flag,
        OORDDTA.ord_paid_flag                             AS ord_paid_flag,
        OORDDTA.org_ord_id                                AS org_ord_id,
        CAST(OORDDTA.ord_type_cd AS      VARCHAR2(240 CHAR))   AS ord_type_cd,
        CAST(OORDDTA.drop_cd AS          VARCHAR2(240 CHAR))   AS drop_cd,
        CAST(OORDDTA.delivery_fee_net AS NUMBER)               AS delivery_fee_net,
        CAST(OORDLIN.ord_ln_id AS        NUMBER(38, 0))        AS ord_ln_id,
        CAST(OORDLIN.ref_ln_id AS        NUMBER(38, 0))        AS ref_ln_id,
        CAST(OORDLIN.ord_ln_disp_cd AS   VARCHAR2(240 CHAR))   AS ord_ln_disp_cd,
        CAST(OORDLIN.ord_item_cd AS      VARCHAR2(960 BYTE))   AS ord_item_cd,
        CAST(OORDLIN.shp_item_cd AS      VARCHAR2(240 CHAR))   AS shp_item_cd,
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))))                                                                -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR))))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(CAST(OORDLIN.ord_item_cd AS VARCHAR2(15 CHAR)))
        END AS ord_item_base_cd,
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))))                                                                -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))), 4, LENGTH(TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR))))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(CAST(OORDLIN.shp_item_cd AS VARCHAR2(15 CHAR)))
        END                                               AS shp_item_base_cd,
        CAST(OORDLIN.adj_ln_lc_net AS NUMBER)             AS adj_ln_lc_net,
        CAST(OORDLIN.adj_ln_pv AS     NUMBER)             AS adj_ln_pv,
        CAST(OORDLIN.adj_ln_bv AS     NUMBER)             AS adj_ln_bv,
        CAST(OORDLIN.ord_qty AS       NUMBER)             AS ord_qty,
        CAST(OORDLIN.shp_qty AS       NUMBER)             AS shp_qty,
        CAST(NULLIF('0','0') AS       VARCHAR2(240 CHAR)) AS delivery_mode_cd,
        CAST(NULLIF('0','0') AS       VARCHAR2(50 CHAR))  AS consultant_cd,
        CAST(SYSDATE AS               TIMESTAMP(6))       AS UPDATE_DT
      FROM OORDLIN OORDLIN
      LEFT JOIN OORDDTA OORDDTA
      ON OORDLIN.ord_id        =OORDDTA.ord_id
      AND OORDLIN.init_cntry_id=OORDDTA.init_cntry_id
        )
