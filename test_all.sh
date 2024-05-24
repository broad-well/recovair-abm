#!/bin/bash

set -Eeuo pipefail
set -x

echo "UPDATE scenarios SET aircraft_selector = NULL" | sqlite3 db/generator/test.db
echo "UPDATE scenarios SET max_delay = 360" | sqlite3 db/generator/test.db

node test_scenario.js 2022-12-22-bts-import results-2022-12-22 > test_log_12-22.log
node test_scenario.js 2022-12-22-bts-import-nodisrupt results-2022-12-22-nodisrupt > test_log_12-22-nodisrupt.log
node test_scenario.js jan28-bts-import results-2024-01-28 > test_log_01-28.log
node test_scenario.js 2024-01-28-bts-import results-2024-01-28-nodisrupt > test_log_01-28-nodisrupt.log

echo "UPDATE scenarios SET aircraft_selector = 'dfs'" | sqlite3 db/generator/test.db

node test_scenario.js 2022-12-22-bts-import results-2022-12-22-aircraftdfs > test_log_12-22-aircraftdfs.log
node test_scenario.js 2022-12-22-bts-import-nodisrupt results-2022-12-22-nodisrupt-aircraftdfs > test_log_12-22-nodisrupt-aircraftdfs.log
node test_scenario.js jan28-bts-import results-2024-01-28-aircraftdfs > test_log_01-28-aircraftdfs.log
node test_scenario.js 2024-01-28-bts-import results-2024-01-28-nodisrupt-aircraftdfs > test_log_01-28-nodisrupt-aircraftdfs.log

echo "UPDATE scenarios SET aircraft_selector = NULL" | sqlite3 db/generator/test.db