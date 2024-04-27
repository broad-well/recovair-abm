PRAGMA foreign_keys = ON;
DROP TABLE IF EXISTS deadheaders;
DROP TABLE IF EXISTS demand;
DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS crew;
DROP TABLE IF EXISTS aircraft;
DROP TABLE IF EXISTS airports;
DROP TABLE IF EXISTS scenarios;
DROP TABLE IF EXISTS disruptions;

CREATE TABLE scenarios(
    sid     TEXT PRIMARY KEY,
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

CREATE TABLE airports(
    code    TEXT NOT NULL,
    max_dep_per_hour INTEGER NOT NULL,
    max_arr_per_hour INTEGER NOT NULL,
    latitude    REAL NOT NULL,
    longitude   REAL NOT NULL,
    sid     TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE,
    PRIMARY KEY(code, sid)
);

CREATE TABLE aircraft(
    tail    TEXT NOT NULL,
    location    TEXT NOT NULL,
    typename    TEXT NOT NULL,
    capacity    INTEGER NOT NULL,
    sid     TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE,
    PRIMARY KEY(tail, sid)
);

CREATE TABLE crew(
    id  INTEGER NOT NULL,
    location    TEXT NOT NULL,
    sid     TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE,
    PRIMARY KEY(id, sid)
);

CREATE TABLE flights(
    id  INTEGER NOT NULL,
    flight_number   TEXT NOT NULL,
    aircraft    TEXT NOT NULL,
    origin  TEXT NOT NULL,
    dest    TEXT NOT NULL,
    pilot   INTEGER,
    sched_depart    TEXT NOT NULL,
    sched_arrive    TEXT NOT NULL,
    sid     TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE,
    FOREIGN KEY(origin, sid) REFERENCES airports(code, sid) ON DELETE CASCADE,
    FOREIGN KEY(dest, sid) REFERENCES airports(code, sid) ON DELETE CASCADE,
    FOREIGN KEY(aircraft, sid) REFERENCES aircraft(tail, sid) ON DELETE CASCADE,
    PRIMARY KEY(id, sid)
);

CREATE TABLE demand(
    path    TEXT NOT NULL, -- delimited by hyphens, like "SAN-LAS-DEN"
    amount  INT NOT NULL, -- number of passengers
    sid     TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);

CREATE TABLE deadheaders(
    id  INTEGER NOT NULL,
    sid INTEGER NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE,
    fid INTEGER NOT NULL,
    FOREIGN KEY(fid, sid) REFERENCES flights(id, sid) ON DELETE CASCADE,
    FOREIGN KEY(id, sid) REFERENCES crew(id, sid) ON DELETE CASCADE
);

CREATE TABLE disruptions(
    airport     TEXT NOT NULL,
    start       TEXT NOT NULL,
    end         TEXT NOT NULL,
    hourly_rate INTEGER NOT NULL,
    type        TEXT NOT NULL,
    reason      TEXT NOT NULL,
    sid         TEXT NOT NULL REFERENCES scenarios(sid) ON DELETE CASCADE
);