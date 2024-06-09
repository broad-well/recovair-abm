//! Defines adapters for constructing scenarios from external sources.

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{mpsc, Arc, RwLock},
};

use chrono::{DateTime, NaiveDateTime, ParseError, TimeDelta, Utc};
use rusqlite::Connection;

use crate::{
    aircraft::{Aircraft, Flight, FlightId},
    airport::{
        Airport, AirportCode, CumulativeSmallSlotManager, DepartureRateLimit, Disruption, DisruptionIndex, GroundDelayProgram, PassengerDemand, SlotManager
    },
    crew::{Crew, CrewId},
    dispatcher::{strategies, Dispatcher},
    metrics::MetricsProcessor,
    model::{Model, ModelConfig},
};

pub trait ScenarioLoader<E: std::fmt::Debug> {
    fn read_model(&self) -> Result<Model, E>;
    fn read_dispatcher(&self, model: Arc<Model>) -> Result<Dispatcher, E>;
}

pub struct SqliteScenarioLoader {
    conn: Connection,
    id: String,
}

impl SqliteScenarioLoader {
    pub fn new(path: &str, scenario_id: String) -> Result<Self, ScenarioLoaderError> {
        Ok(Self {
            conn: Connection::open(path)?,
            id: scenario_id,
        })
    }
}

#[derive(Debug)]
pub enum ScenarioLoaderError {
    DatabaseError(rusqlite::Error),
    MissingRequiredDataError(&'static str),
    FormatError(ParseError),
}

impl From<rusqlite::Error> for ScenarioLoaderError {
    fn from(value: rusqlite::Error) -> Self {
        Self::DatabaseError(value)
    }
}
impl From<ParseError> for ScenarioLoaderError {
    fn from(value: ParseError) -> Self {
        Self::FormatError(value)
    }
}

impl ScenarioLoader<ScenarioLoaderError> for SqliteScenarioLoader {
    fn read_model(&self) -> Result<Model, ScenarioLoaderError> {
        let (now, end, config) = self.read_config()?;
        let (tx, rx) = mpsc::channel();
        let mut model = Model {
            airports: HashMap::new(),
            fleet: HashMap::new(),
            crew: HashMap::new(),
            flights: HashMap::new(),
            disruptions: DisruptionIndex::new(),
            _now: Arc::new(RwLock::new(now)),
            end,
            publisher: tx,
            metrics: RwLock::new(Some(MetricsProcessor::new(rx))),
            config,
        };
        self.read_airports(&mut model)?;
        self.read_aircraft(&mut model)?;
        self.read_crew(&mut model)?;
        self.read_flights(&mut model)?;
        self.read_demand(&mut model)?;
        self.read_disruptions(&mut model)?;
        Ok(model)
    }

    fn read_dispatcher(&self, model: Arc<Model>) -> Result<Dispatcher, ScenarioLoaderError> {
        let mut stmt = self.conn.prepare(
            "SELECT aircraft_selector, crew_selector, wait_for_deadheaders, aircraft_reassign_tolerance, crew_reassign_tolerance FROM scenarios WHERE sid = (?1)")?;
        let mut rows = stmt.query([&self.id])?;
        let Some(row) = rows.next()? else {
            return Err(ScenarioLoaderError::MissingRequiredDataError(
                "Missing config info",
            ));
        };

        let asel: Option<String> = row.get("aircraft_selector")?;
        let asel = asel.map(|asel| strategies::new_for_aircraft(&asel));
        let csel: Option<String> = row.get("crew_selector")?;
        let csel = csel.map(|csel| strategies::new_for_crew(&csel));

        Ok(Dispatcher {
            model,
            aircraft_selector: asel,
            crew_selector: csel,
            wait_for_deadheaders: row.get::<&str, i32>("wait_for_deadheaders")? > 0i32,
            aircraft_tolerance_before_reassign: TimeDelta::minutes(
                row.get("aircraft_reassign_tolerance")?,
            ),
            use_fallback_aircraft_selector: true, // TODO add adjuster
            crew_tolerance_before_reassign: TimeDelta::minutes(row.get("crew_reassign_tolerance")?),
            update_queue: BinaryHeap::new(),
            aircraft_reassigned: HashSet::new(),
        })
    }
}

impl SqliteScenarioLoader {
    const TIME_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    fn read_airports(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self.conn.prepare(
            "SELECT code, max_dep_per_hour, max_arr_per_hour FROM airports WHERE sid = (?1)",
        )?;
        let mut query = stmt.query([&self.id])?;
        while let Some(row) = query.next()? {
            let code = AirportCode::from(&row.get("code")?);
            model.airports.insert(
                code,
                Arc::new(RwLock::new(Airport {
                    code,
                    fleet: HashSet::new(),
                    crew: HashSet::new(),
                    passengers: Vec::new(),
                    max_arr_per_hour: row.get("max_arr_per_hour")?,
                    max_dep_per_hour: row.get("max_dep_per_hour")?,
                    departure_count: (model.now(), 0),
                    arrival_count: (model.now(), 0),
                })),
            );
        }
        Ok(())
    }

    fn read_aircraft(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self
            .conn
            .prepare("SELECT tail, location, typename, capacity FROM aircraft WHERE sid = (?1)")?;
        let mut query = stmt.query([&self.id])?;
        while let Some(row) = query.next()? {
            let tail: String = row.get("tail")?;
            let location = AirportCode::from(&row.get("location")?);
            model.fleet.insert(
                tail.clone(),
                Arc::new(RwLock::new(Aircraft::new(
                    tail.clone(),
                    location,
                    &model.now(),
                    row.get("typename")?,
                    row.get("capacity")?,
                ))),
            );
            model.airports[&location]
                .write()
                .unwrap()
                .fleet
                .insert(tail);
        }
        Ok(())
    }

    fn read_crew(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, location FROM crew WHERE sid = ?1")?;
        let mut rows = stmt.query([&self.id])?;
        while let Some(row) = rows.next()? {
            let cid: CrewId = row.get("id")?;
            let location = AirportCode::from(&row.get("location")?);
            model.crew.insert(
                cid,
                Arc::new(RwLock::new(Crew::new(cid, location, model.now()))),
            );
            model.airports[&location].write().unwrap().crew.insert(cid);
        }
        Ok(())
    }

    fn read_flights(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, flight_number, aircraft, origin, pilot, dest, sched_depart, sched_arrive FROM flights WHERE sid = ?1")?;
        let mut rows = stmt.query([&self.id])?;
        while let Some(row) = rows.next()? {
            let mut crews_query = self
                .conn
                .prepare_cached("SELECT id FROM deadheaders WHERE sid = ?1 AND fid = ?2")?;
            let flight_id: FlightId = row.get("id")?;
            let mut crews_rows = crews_query.query(rusqlite::params![&self.id, flight_id])?;
            let mut flight = Flight {
                id: flight_id,
                flight_number: row.get("flight_number")?,
                aircraft_tail: row.get("aircraft")?,
                origin: AirportCode::from(&row.get("origin")?),
                dest: AirportCode::from(&row.get("dest")?),
                crew: {
                    let pilot: Option<CrewId> = row.get("pilot")?;
                    pilot.map(|i| vec![i]).unwrap_or(Vec::new())
                },
                passengers: Vec::new(),
                cancelled: false,
                depart_time: None,
                arrive_time: None,
                dep_delay: TimeDelta::zero(),
                accum_delay: None,
                sched_depart: Self::parse_time(&row.get::<&str, String>("sched_depart")?)?,
                sched_arrive: Self::parse_time(&row.get::<&str, String>("sched_arrive")?)?,
            };
            while let Some(deadheader_row) = crews_rows.next()? {
                flight.crew.push(deadheader_row.get("id")?);
            }
            model
                .flights
                .insert(flight_id, Arc::new(RwLock::new(flight)));
        }
        Ok(())
    }

    fn read_config(
        &self,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>, ModelConfig), ScenarioLoaderError> {
        let mut stmt = self.conn.prepare(
            "SELECT start_time, end_time, crew_turnaround_time, aircraft_turnaround_time, max_delay FROM scenarios WHERE sid = (?1)")?;
        let mut rows = stmt.query([&self.id])?;
        let Some(row) = rows.next()? else {
            return Err(ScenarioLoaderError::MissingRequiredDataError(
                "Missing config info",
            ));
        };

        let start_time_str: String = row.get("start_time")?;
        let start = Self::parse_time(&start_time_str)?;
        let end_time_str: String = row.get("end_time")?;
        let end = Self::parse_time(&end_time_str)?;
        Ok((
            start,
            end,
            ModelConfig {
                crew_turnaround_time: TimeDelta::minutes(row.get("crew_turnaround_time")?),
                aircraft_turnaround_time: TimeDelta::minutes(row.get("aircraft_turnaround_time")?),
                max_delay: TimeDelta::minutes(row.get("max_delay")?),
            },
        ))
    }

    fn read_demand(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self
            .conn
            .prepare("SELECT path, amount FROM demand WHERE sid = ?1")?;
        let mut rows = stmt.query([&self.id])?;

        while let Some(row) = rows.next()? {
            let path_str: String = row.get("path")?;
            let demand = PassengerDemand {
                path: path_str
                    .split('-')
                    .map(|string| AirportCode::from(&string.to_owned()))
                    .collect(),
                count: row.get("amount")?,
                flights_taken: Vec::new(),
            };
            if demand.count > 0 {
                model
                    .airports
                    .get(&demand.path[0])
                    .unwrap()
                    .write()
                    .unwrap()
                    .passengers
                    .push(demand);
            }
        }
        Ok(())
    }

    fn read_disruptions(&self, model: &mut Model) -> Result<(), ScenarioLoaderError> {
        let mut stmt = self.conn.prepare(
            "SELECT airport, start, end, hourly_rate, type, reason FROM disruptions WHERE sid = ? ORDER BY airport, type, start ASC",
        )?;
        let mut rows = stmt.query([&self.id])?;

        if let Some(first_row) = rows.next()? {
            let mut ongoing_reason: String = first_row.get("reason")?;
            let mut ongoing_site = AirportCode::from(&first_row.get("airport")?);
            let mut ongoing_type: String = first_row.get("type")?;
            let mut ongoing_start = Self::parse_time(&first_row.get::<&str, String>("start")?)?;
            let mut ongoing_end = Self::parse_time(&first_row.get::<&str, String>("end")?)?;
            let mut ongoing_rates: Vec<u32> = std::iter::repeat(first_row.get("hourly_rate")?)
                .take((ongoing_end - ongoing_start).num_hours() as usize)
                .collect();

            while let Some(row) = rows.next()? {
                let start = Self::parse_time(&row.get::<&str, String>("start")?)?;
                let end = Self::parse_time(&row.get::<&str, String>("end")?)?;
                let rate: u32 = row.get("hourly_rate")?;
                let _type: String = row.get("type")?;
                let site = AirportCode::from(&row.get("airport")?);

                if site != ongoing_site || _type != ongoing_type || start != ongoing_end {
                    // The ongoing CSSM is ready to be built
                    println!("Disruption reading debug: read {:?} for {:?} (type = {})", ongoing_rates, ongoing_site, ongoing_type);
                    let slot_man = CumulativeSmallSlotManager::<FlightId>::new(ongoing_start, ongoing_rates);
                    println!("{:?}", slot_man.hourly_accumulation_limit);
                    let disruption: Arc<RwLock<dyn Disruption>> = match ongoing_type.as_str() {
                        "gdp" => Arc::new(RwLock::new(GroundDelayProgram {
                            site: ongoing_site,
                            slots: slot_man,
                            reason: Some(ongoing_reason),
                        })),
                        "dep" => Arc::new(RwLock::new(DepartureRateLimit {
                            site: ongoing_site,
                            slots: slot_man,
                            reason: Some(ongoing_reason),
                        })),
                        _ => {
                            return Err(ScenarioLoaderError::MissingRequiredDataError(
                                "unknown disruption type",
                            ))
                        }
                    };
                    model.disruptions.add_disruption(disruption);
                    ongoing_rates = Vec::new();
                    ongoing_site = site;
                    ongoing_type = _type;
                    ongoing_start = start;
                    ongoing_reason = row.get("reason")?;
                }
                ongoing_end = end;
                ongoing_rates.extend(std::iter::repeat(rate).take((end - start).num_hours() as usize));
            }
            // TODO fix duplication
            println!("Disruption reading debug: read {:?} for {:?} (type = {})", ongoing_rates, ongoing_site, ongoing_type);
            let slot_man = CumulativeSmallSlotManager::<FlightId>::new(ongoing_start, ongoing_rates);
            println!("{:?}", slot_man.hourly_accumulation_limit);
            let disruption: Arc<RwLock<dyn Disruption>> = match ongoing_type.as_str() {
                "gdp" => Arc::new(RwLock::new(GroundDelayProgram {
                    site: ongoing_site,
                    slots: slot_man,
                    reason: Some(ongoing_reason),
                })),
                "dep" => Arc::new(RwLock::new(DepartureRateLimit {
                    site: ongoing_site,
                    slots: slot_man,
                    reason: Some(ongoing_reason),
                })),
                _ => {
                    return Err(ScenarioLoaderError::MissingRequiredDataError(
                        "unknown disruption type",
                    ))
                }
            };
            model.disruptions.add_disruption(disruption);
        }
        Ok(())
    }

    fn parse_time(time: &str) -> Result<DateTime<Utc>, ScenarioLoaderError> {
        Ok(NaiveDateTime::parse_from_str(time, Self::TIME_FORMAT)?.and_utc())
    }
}
