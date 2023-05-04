view: cummulative_run_hour_report_2_new {
  sql_table_name: `indus-tower.tower.cummulative_run_hour_report_2_NEW`
    ;;

  dimension: avg_load {
    type: number
    sql: ${TABLE}.AvgLoad ;;
  }

  dimension: diesel_consumption {
    type: number
    sql: ${TABLE}.dieselConsumption ;;
  }

  dimension: rtu {
    type: string
    sql: ${TABLE}.rtu ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.siteId ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
explore: cummulative_run_hour_report_2_new {}
