connection: "indus-tower"
include: "/views/**/*.view"

datagroup: indus-tower_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: indus-tower_default_datagroup
