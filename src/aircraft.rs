use std::cmp::max;

use crate::crew::CrewId;
use crate::model::Model;
use crate::{airport::*, metrics::ModelEventType};
use chrono::{DateTime, TimeDelta, Utc};

pub type FlightId = u64;

/// Represents a flight (either planned or executed).
/// If `arrive_time` is None, then this must be a flight in progress.
///
/// Owner: Model
#[derive(Debug)]
pub struct Flight {
    pub id: FlightId,
    pub flight_number: String,
    pub aircraft_tail: Option<String>,
    /// First element is piloting, the rest are deadheading
    pub crew: Vec<CrewId>,
    /// If empty, then this is a ferry flight
    pub passengers: Vec<PassengerDemand>,
    pub origin: AirportCode,
    pub dest: AirportCode,
    pub cancelled: bool,
    pub depart_time: Option<DateTime<Utc>>,
    pub arrive_time: Option<DateTime<Utc>>,
    pub dep_delay: TimeDelta,
    pub accum_delay: Option<TimeDelta>,
    pub sched_depart: DateTime<Utc>,
    pub sched_arrive: DateTime<Utc>,
}

impl PartialEq for Flight {
    fn eq(&self, other: &Self) -> bool {
        self.flight_number == other.flight_number
    }
}

impl Flight {
    #[inline]
    pub fn took_off(&self) -> bool {
        self.depart_time.is_some()
    }

    pub fn est_arrive_time(&self, depart: &DateTime<Utc>) -> DateTime<Utc> {
        *depart
            + (self.est_duration() + self.accum_delay.unwrap_or(TimeDelta::zero()))
    }

    pub fn act_arrive_time(&self) -> DateTime<Utc> {
        self.est_arrive_time(
            &self
                .depart_time
                .expect("Don't call act_arrive_time on a flight that has not departed"),
        )
    }

    #[inline]
    pub fn est_duration(&self) -> TimeDelta {
        self.sched_arrive - self.sched_depart
    }

    pub fn takeoff(&mut self, time: DateTime<Utc>) {
        self.depart_time = Some(time);
    }

    pub fn land(&mut self, time: DateTime<Utc>) {
        self.arrive_time = Some(time);
    }

    pub fn delay_departure(&mut self, delay: TimeDelta) {
        self.dep_delay += delay;
    }

    pub fn delay_arrival(&mut self, duration: TimeDelta) {
        self.accum_delay = Some(self.accum_delay.unwrap_or(TimeDelta::zero()) + duration);
    }

    pub fn reassign_aircraft(&mut self, tail: String) -> bool {
        debug_assert!(!self.took_off());
        if self.aircraft_tail.as_ref() == Some(&tail) {
            false
        } else {
            self.aircraft_tail = Some(tail);
            true
        }
    }

    pub fn reassign_crew(&mut self, id: Vec<CrewId>) {
        debug_assert!(!id.is_empty());
        self.crew = id;
    }
}

/// Owner: Aircraft or Crew
#[derive(Debug)]
pub enum Location {
    /// On the ground at airport `self.0` since time `self.1`.
    Ground(AirportCode, DateTime<Utc>),
    InFlight(FlightId),
}

#[derive(Debug)]
pub struct Aircraft {
    pub tail: String,
    pub location: Location,
    /// (Name, passenger capacity)
    pub type_: (String, u16),
    pub next_claimed: Option<FlightId>,
}

impl Aircraft {
    pub fn new(
        tail: String,
        location: AirportCode,
        now: &DateTime<Utc>,
        typename: String,
        capacity: u16,
    ) -> Self {
        Aircraft {
            tail,
            location: Location::Ground(location, *now - TimeDelta::hours(2)),
            type_: (typename, capacity),
            next_claimed: None,
        }
    }

    pub fn takeoff(&mut self, flight_id: FlightId, time: DateTime<Utc>) -> ModelEventType {
        let Location::Ground(airport, since) = self.location else {
            panic!(
                "takeoff({}) called on {} when aircraft in the air",
                flight_id, self.tail
            )
        };
        self.location = Location::InFlight(flight_id);
        self.next_claimed = None;
        ModelEventType::AircraftTurnedAround(self.tail.clone(), airport, time - since)
    }

    pub fn land(&mut self, loc: AirportCode, time: DateTime<Utc>) {
        self.location = Location::Ground(loc, time);
    }

    pub fn available_time(&self, model: &Model, flight: &Flight) -> Option<DateTime<Utc>> {
        let airport = flight.origin;
        if let Some(claimer) = self.next_claimed {
            if claimer != flight.id {
                return None;
            }
        }
        if let Location::Ground(_airport, since_time) = self.location {
            if _airport == airport {
                Some(since_time + model.config.aircraft_turnaround_time)
            } else {
                None
            }
        } else if let Location::InFlight(flt_id) = self.location {
            let flt = model.flight_read(flt_id);
            if flt.dest == airport {
                // flt.act_arrive_time() may be before model.now() due to arrival delays
                Some(
                    max(model.now(), flt.act_arrive_time()) + model.config.aircraft_turnaround_time,
                )
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn claim(&mut self, flight: FlightId) {
        debug_assert_eq!(self.next_claimed, None);
        self.next_claimed = Some(flight);
    }

    pub fn unclaim(&mut self, flight: FlightId) {
        if self.next_claimed == Some(flight) {
            self.next_claimed = None;
        }
    }
}
