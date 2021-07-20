#!/bin/bash -e

log_path="$1"
config_path="$2"

spark-submit \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --master local[*] \
    main.py "$log_path" "$config_path"
    