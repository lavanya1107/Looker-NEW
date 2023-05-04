view: sql_runner_query_new_cumulative__run_hour {
  derived_table: {
    sql: WITH
        cte AS(
        SELECT
          UPPER(a.siteId) AS siteId,
          DATETIME(a.minTime) Start_date,
          DATETIME(a.maxTime) End_Date,
          --DATE(timestamp) AS eventDate,
          ROUND((a.mainRunMax-IFNULL(a.mainRunMin,
              0)),2) AS MainRunHrs,
        ROUND((a.dgRunMax-IFNULL(a.dgRunMin,
              0)),2) AS DgTotRunHrs,
        ROUND((a.battRunMax-IFNULL(a.battRunMin,
              0)),2) AS BattRunHrs,
        ROUND(ROUND((a.mainRunMax-IFNULL(a.mainRunMin,
                0)),2)+ROUND((a.dgRunMax-IFNULL(a.dgRunMin,
                0)),2)+ROUND((a.battRunMax-IFNULL(a.battRunMin,
                0)),2),2) AS TotalRunHrs,
        ROUND((TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), MINUTE)/60),0) AS streamTime,
        (
          CASE
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >=0 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=4 THEN '0_4Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >4
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=8 THEN '4_8Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >8 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) <=12 THEN '8_12Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >12
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=16 THEN '12_16Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >16 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=20 THEN '16_20Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >20
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=24 THEN '20_24Hrs'
          ELSE
          '>24Hrs'
        END
          ) AS Runhr_Bucket,
        rtuType
        FROM (
          SELECT
            siteId,
            'IIPMS' AS rtuType,
            MIN(timestamp) minTime,
            MAX(timestamp) maxTime,
            MIN(NULLIF(CASE WHEN mainRunHrs IN (-9999,-1111,0) THEN 0 ELSE mainRunHrs END,0)) mainRunMin,
            MAX(mainRunHrs) mainRunMax,
            MIN(NULLIF(CASE WHEN dgTotRunHrs IN (-9999,-1111,0) THEN 0 ELSE dgTotRunHrs END,0)) dgRunMin,
            MAX(dgTotRunHrs) dgRunMax,
            MIN(NULLIF(CASE WHEN battRunHrs IN (-9999,-1111,0) THEN 0 ELSE battRunHrs END,0)) battRunMin,
            MAX(battRunHrs) battRunMax
          FROM
            `indus-tower.tower.device_telemetry`
          WHERE
            UPPER(faultPeriodic)='P'
            AND DATE(timestamp) between DATE_SUB(CURRENT_DATE(), interval 30 day) and CURRENT_DATE()
          GROUP BY
            siteId ) AS a
      WHERE
        ROUND((a.mainRunMax-IFNULL(a.mainRunMin,0)),2) <=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND((a.dgRunMax-IFNULL(a.dgRunMin,0)),2)<=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND((a.battRunMax-IFNULL(a.battRunMin,0)),2)<=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND(ROUND((a.mainRunMax-IFNULL(a.mainRunMin,0)),2)+
        ROUND((a.dgRunMax-IFNULL(a.dgRunMin,0)),2)+
        ROUND((a.battRunMax-IFNULL(a.battRunMin,0)),2),2) > 0
        UNION ALL
        SELECT
        UPPER(a.siteId) AS siteId,
        DATETIME(a.minTime) Start_date,
        DATETIME(a.maxTime) End_Date,
        --DATE(timestamp) AS eventDate,
        ROUND((a.mainRunMax-IFNULL(a.mainRunMin,
              0)),2) AS MainRunHrs,
        ROUND((a.dgRunMax-IFNULL(a.dgRunMin,
              0)),2) AS DgTotRunHrs,
        ROUND((a.battRunMax-IFNULL(a.battRunMin,
              0)),2) AS BattRunHrs,
        ROUND(ROUND((a.mainRunMax-IFNULL(a.mainRunMin,
                0)),2)+ROUND((a.dgRunMax-IFNULL(a.dgRunMin,
                0)),2)+ROUND((a.battRunMax-IFNULL(a.battRunMin,
                0)),2),2) AS TotalRunHrs,
        ROUND((TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), MINUTE)/60),0) AS streamTime,
        (
          CASE
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >=0 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=4 THEN '0_4Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >4
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=8 THEN '4_8Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >8 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) <=12 THEN '8_12Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >12
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=16 THEN '12_16Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >16 AND (((a.mainRunMax - IFNULL(a.mainRunMin, 0))+ (a.dgRunMax - IFNULL(a.dgRunMin, 0)) + (a.battRunMax-IFNULL(a.battRunMin, 0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=20 THEN '16_20Hrs'
            WHEN (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)) >20
          AND (((a.mainRunMax - IFNULL(a.mainRunMin,
                  0))+ (a.dgRunMax - IFNULL(a.dgRunMin,
                  0)) + (a.battRunMax-IFNULL(a.battRunMin,
                  0)))/ (TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1))<=24 THEN '20_24Hrs'
          ELSE
          '>24Hrs'
        END
          ) AS Runhr_Bucket,
        rtuType
      FROM (
        SELECT
          siteId,
          'U-Modem' AS rtuType,
          MIN(timestamp) minTime,
          MAX(timestamp) maxTime,
          MIN(NULLIF(CASE WHEN mainRunHrs IN (-9999,-1111,0) THEN 0 ELSE mainRunHrs END,0)) as mainRunMin,
          MAX(mainRunHrs) mainRunMax,
          MIN(NULLIF(CASE WHEN dgTotRunHrs IN (-9999,-1111,0) THEN 0 ELSE dgTotRunHrs END,0)) dgRunMin,
          MAX(dgTotRunHrs) dgRunMax,
          MIN(NULLIF(CASE WHEN battRunHrs IN (-9999,-1111,0) THEN 0 ELSE battRunHrs END,0)) battRunMin,
          MAX(battRunHrs) battRunMax
        FROM
          `indus-tower.tower.device_telemetry_temp`
        WHERE
          UPPER(faultPeriodic)='P'
          AND DATE(timestamp) between DATE_SUB(CURRENT_DATE(), interval 30 day) and CURRENT_DATE()
          AND device_type ="SPS"
          AND siteId IN (
          SELECT
            DISTINCT IFNULL(SERVICE_ID,
              "null") AS SERVICE_ID
          FROM
            `indus-tower.tower.device_state_data_mqtt`
          WHERE
            MODEM_ID NOT IN ("device-1",
              "device-2",
              "device-3",
              "device-4",
              "device-5",
              "device-6",
              "device-7",
              "device-8",
              "device-9",
              "device-10")
            AND CHAR_LENGTH(SERVICE_ID) = 10)
        GROUP BY
          siteId ) AS a
      WHERE
        ROUND((a.mainRunMax-IFNULL(a.mainRunMin,0)),2) <=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND((a.dgRunMax-IFNULL(a.dgRunMin,0)),2)<=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND((a.battRunMax-IFNULL(a.battRunMin,0)),2)<=30*(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(a.maxTime)),TIMESTAMP(DATETIME(a.minTime)), Day)+1)
        AND ROUND(ROUND((a.mainRunMax-IFNULL(a.mainRunMin,0)),2)+
        ROUND((a.dgRunMax-IFNULL(a.dgRunMin,0)),2)+
        ROUND((a.battRunMax-IFNULL(a.battRunMin,0)),2),2) > 0 )

      SELECT
      "Run_Hour" as RUN_HOUR,
      IFNULL(siteMap.circle,
      "NULL") circle,
      IFNULL(siteMap.district,
      "NULL") district,
      IFNULL(siteMap.cm,
      "NULL") cm,
      IFNULL(siteMap.zonal_head,
      "NULL") zonal_head,
      IFNULL(siteMap.fse,
      "NULL") fse,
      IFNULL(siteMap.technician,
      "NULL") technician,
      cte.*,
      ((TIMESTAMP_DIFF(cte.End_Date,cte.Start_date,MINUTE)/60)/24) AS time_diff,
      (CASE
      WHEN diesel.diesel_Vol IS NULL THEN 0
      ELSE
      diesel.diesel_Vol
      END
      ) AS diesel_fill
      FROM
      cte
      LEFT JOIN
      `indus-tower.tower.Site_Mapping` siteMap
      ON
      UPPER(cte.siteId) = UPPER(siteMap.siteId)
      LEFT JOIN (
      SELECT
      UPPER(siteId) AS siteId,
      SUM(diesel_ltr ) diesel_Vol
      FROM
      `indus-tower.tower.Diesel_Filing_Table`
      WHERE
      DATE(date_time) between DATE_SUB(CURRENT_DATE(), interval 30 day) and CURRENT_DATE()
      GROUP BY
      UPPER(siteId)) diesel
      ON
      cte.siteId = diesel.siteId
      WHERE
      REGEXP_CONTAINS(cte.siteId,r'^IN-\d{7}')
      ORDER BY
      Start_date DESC
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: run_hour {
    type: string
    sql: ${TABLE}.RUN_HOUR ;;
  }

  dimension: circle {
    type: string
    sql: ${TABLE}.circle ;;
  }

  dimension: district {
    type: string
    sql: ${TABLE}.district ;;
  }

  dimension: cm {
    type: string
    sql: ${TABLE}.cm ;;
  }

  dimension: zonal_head {
    type: string
    sql: ${TABLE}.zonal_head ;;
  }

  dimension: fse {
    type: string
    sql: ${TABLE}.fse ;;
  }

  dimension: technician {
    type: string
    sql: ${TABLE}.technician ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.siteId ;;
  }

  dimension_group: start_date {
    type: time
    datatype: datetime
    sql: ${TABLE}.Start_date ;;
  }

  dimension_group: end_date {
    type: time
    datatype: datetime
    sql: ${TABLE}.End_Date ;;
  }

  dimension: main_run_hrs {
    type: number
    sql: ${TABLE}.MainRunHrs ;;
  }

  dimension: dg_tot_run_hrs {
    type: number
    sql: ${TABLE}.DgTotRunHrs ;;
  }

  dimension: batt_run_hrs {
    type: number
    sql: ${TABLE}.BattRunHrs ;;
  }

  dimension: total_run_hrs {
    type: number
    sql: ${TABLE}.TotalRunHrs ;;
  }

  dimension: stream_time {
    type: number
    sql: ${TABLE}.streamTime ;;
  }

  dimension: runhr_bucket {
    type: string
    sql: ${TABLE}.Runhr_Bucket ;;
  }

  dimension: rtu_type {
    type: string
    sql: ${TABLE}.rtuType ;;
  }

  dimension: time_diff {
    type: number
    sql: ${TABLE}.time_diff ;;
  }

  dimension: diesel_fill {
    type: number
    sql: ${TABLE}.diesel_fill ;;
  }

  set: detail {
    fields: [
      run_hour,
      circle,
      district,
      cm,
      zonal_head,
      fse,
      technician,
      site_id,
      start_date_time,
      end_date_time,
      main_run_hrs,
      dg_tot_run_hrs,
      batt_run_hrs,
      total_run_hrs,
      stream_time,
      runhr_bucket,
      rtu_type,
      time_diff,
      diesel_fill
    ]
  }
}
