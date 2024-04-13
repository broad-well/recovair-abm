CREATE TABLE IF NOT EXISTS scenarios(
    sid     INTEGER PRIMARY KEY,
    name    TEXT NOT NULL,
    start_time  TEXT NOT NULL,
    end_time    TEXT NOT NULL,
    creation_time   TEXT DEFAULT(CURRENT_TIMESTAMP),
    crew_turnaround_time    INTEGER DEFAULT(30),
    aircraft_turnaround_time    INTEGER DEFAULT(30),
    max_delay   INTEGER DEFAULT(360),
    -- Dispatcher settings
    aircraft_selector   TEXT,
    crew_selector       TEXT,
    wait_for_deadheaders    INTEGER DEFAULT(0),
    aircraft_reassign_tolerance INTEGER DEFAULT(120),
    crew_reassign_tolerance INTEGER DEFAULT(120)
);

CREATE TABLE IF NOT EXISTS airports(
    code    TEXT NOT NULL,
    max_dep_per_hour INTEGER NOT NULL,
    max_arr_per_hour INTEGER NOT NULL,
    sid     INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS aircraft(
    tail    TEXT NOT NULL,
    location    TEXT NOT NULL,
    typename    TEXT NOT NULL,
    capacity    INTEGER NOT NULL,
    sid     INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crew(
    id  INTEGER NOT NULL,
    location    TEXT NOT NULL,
    sid     INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flights(
    id  INTEGER NOT NULL,
    flight_number   TEXT NOT NULL,
    aircraft    TEXT NOT NULL,
    origin  TEXT NOT NULL,
    dest    TEXT NOT NULL,
    pilot   INTEGER NOT NULL,
    sched_depart    TEXT NOT NULL,
    sched_arrive    TEXT NOT NULL,
    sid     INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS demand(
    path    TEXT NOT NULL, -- delimited by hyphens, like "SAN-LAS-DEN"
    amount  INT NOT NULL, -- number of passengers
    sid     INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);