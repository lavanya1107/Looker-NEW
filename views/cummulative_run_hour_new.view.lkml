view: cummulative_run_hour_new {
  sql_table_name: `indus-tower.tower.cummulative_run_hour_NEW`
    ;;

  dimension: batt_run_hrs {
    type: number
    sql: ${TABLE}.BattRunHrs ;;
  }

  dimension: circle {
    type: string
    sql: ${TABLE}.circle ;;
  }

  dimension: cm {
    type: string
    sql: ${TABLE}.cm ;;
  }

  dimension: dg_tot_run_hrs {
    type: number
    sql: ${TABLE}.DgTotRunHrs ;;
  }

  dimension: diesel_fill {
    type: number
    sql: ${TABLE}.diesel_fill ;;
  }

  dimension: district {
    type: string
    sql: ${TABLE}.district ;;
  }

  dimension_group: end {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    datatype: datetime
    sql: ${TABLE}.End_Date ;;
  }

  dimension: fse {
    type: string
    sql: ${TABLE}.fse ;;
  }

  dimension: main_run_hrs {
    type: number
    sql: ${TABLE}.MainRunHrs ;;
  }

  dimension: rtu_type {
    type: string
    sql: ${TABLE}.rtuType ;;
  }

  dimension: run_hour {
    type: string
    sql: ${TABLE}.RUN_HOUR ;;
  }

  dimension: runhr_bucket {
    type: string
    sql: ${TABLE}.Runhr_Bucket ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.siteId ;;
  }

  dimension_group: start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    datatype: datetime
    sql: ${TABLE}.Start_date ;;
  }

  dimension: stream_time {
    type: number
    sql: ${TABLE}.streamTime ;;
  }

  dimension: technician {
    type: string
    sql: ${TABLE}.technician ;;
  }

  dimension: time_diff {
    type: number
    sql: ${TABLE}.time_diff ;;
  }

  dimension: total_run_hrs {
    type: number
    sql: ${TABLE}.TotalRunHrs ;;
  }

  dimension: zonal_head {
    type: string
    sql: ${TABLE}.zonal_head ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
  measure: site_id_for_count{
    type: number
    sql: COUNT(${site_id}) ;;
  }
}
explore: cummulative_run_hour_new {}
