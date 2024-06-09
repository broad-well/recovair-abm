#!/bin/bash

set -Eeuo pipefail
set -x

for threshold in "10" "30" "60" "120" "180" "240" "300" "360"; do
    echo "UPDATE scenarios SET max_delay = $threshold" | sqlite3 db/generator/test.db
    echo "UPDATE scenarios SET aircraft_selector = NULL" | sqlite3 db/generator/test.db

    node test_scenario.js 2022-12-22-bts-import results-2022-12-22_$threshold > test_log_12-22_$threshold.log
    node test_scenario.js 2022-12-22-bts-import-nodisrupt results-2022-12-22_$threshold-nodisrupt > test_log_12-22-nodisrupt_$threshold.log
    node test_scenario.js 2024-01-28-bts-import results-2024-01-28_$threshold > test_log_01-28_$threshold.log
    node test_scenario.js 2024-01-28-bts-import-nodisrupt results-2024-01-28_$threshold-nodisrupt > test_log_01-28-nodisrupt_$threshold.log


    echo "UPDATE scenarios SET aircraft_selector = 'dfs'" | sqlite3 db/generator/test.db

    node test_scenario.js 2022-12-22-bts-import results-2022-12-22-aircraftdfs_$threshold > test_log_12-22-aircraftdfs_$threshold.log
    node test_scenario.js 2024-01-28-bts-import results-2024-01-28-aircraftdfs_$threshold > test_log_01-28-aircraftdfs_$threshold.log

    echo "UPDATE scenarios SET aircraft_selector = NULL" | sqlite3 db/generator/test.db
done