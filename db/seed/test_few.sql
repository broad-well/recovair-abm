-- The scenario is called "few".

INSERT INTO scenarios(sid, name, start_time, end_time, aircraft_selector, crew_selector)
VALUES ('few', "few", "2024-03-01 04:00:00", "2024-03-02 08:00:00", NULL, NULL);

BEGIN TRANSACTION;
INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, sid)
VALUES ('DEN', 114, 114, 'few');
INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, sid)
VALUES ('MDW', 36, 36, 'few');
INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, sid)
VALUES ('DAL', 67, 67, 'few');
INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, sid)
VALUES ('SAN', 24, 24, 'few');
INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, sid)
VALUES ('LAS', 44, 44, 'few');
COMMIT;


BEGIN TRANSACTION;
INSERT INTO aircraft(tail, location, typename, capacity, sid)
VALUES ('N241WN', 'DEN', 'B738', 170, 'few');
INSERT INTO aircraft(tail, location, typename, capacity, sid)
VALUES ('N443WN', 'DEN', 'B738', 170, 'few');
INSERT INTO aircraft(tail, location, typename, capacity, sid)
VALUES ('N941WN', 'SAN', 'B738', 170, 'few');
COMMIT;

BEGIN TRANSACTION;
INSERT INTO crew(id, location, sid)
VALUES (24, 'DEN', 'few');
INSERT INTO crew(id, location, sid)
VALUES (26, 'DEN', 'few');
INSERT INTO crew(id, location, sid)
VALUES (81, 'SAN', 'few');
COMMIT;

BEGIN TRANSACTION;
INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid)
VALUES (1, 'WN2413', 'N241WN', 'DEN', 'LAS', NULL, "2024-03-01 14:30:00", "2024-03-01 16:30:00", 'few');
INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid)
VALUES (2, 'WN441', 'N443WN', 'DEN', 'MDW', NULL, "2024-03-01 10:00:00", "2024-03-01 13:45:00", 'few');
INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid)
VALUES (3, 'WN881', 'N941WN', 'SAN', 'DAL', NULL, "2024-03-01 12:30:00", "2024-03-01 16:45:00", 'few');
INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid)
VALUES (4, 'WN881', 'N941WN', 'DAL', 'MDW', NULL, "2024-03-01 16:45:00", "2024-03-01 19:25:00", 'few');
COMMIT;


BEGIN TRANSACTION;
INSERT INTO demand(path, amount, sid)
VALUES ('SAN-DAL-MDW', 140, 'few');
INSERT INTO demand(path, amount, sid)
VALUES ('DEN-LAS', 130, 'few');
COMMIT;

