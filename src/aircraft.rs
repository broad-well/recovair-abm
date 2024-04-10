use crate::airport::*;
use chrono::{DateTime, Utc};
use crate::crew::CrewId;

/// Represents a flight (either planned or executed).
/// If `arrive_time` is None, then this must be a flight in progress.
///
/// Owner: Model
#[derive(Debug)]
pub struct Flight {
    /// If none, then ferry flight
    pub flight_number: Option<String>,
    pub aircraft_tail: String,
    /// First element is piloting, the rest are deadheading
    pub crew: Vec<CrewId>,
    pub origin: AirportCode,
    pub dest: AirportCode,
    pub depart_time: Option<DateTime<Utc>>,
    pub arrive_time: Option<DateTime<Utc>>,

    pub sched_depart: DateTime<Utc>,
    pub sched_arrive: DateTime<Utc>
}

impl Flight {
    #[inline]
    pub fn in_flight(&self) -> bool {
        self.arrive_time.is_none()
    }
}

/// Owner: Aircraft or Crew
#[derive(Debug)]
pub enum Location<'a> {
    Ground(AirportCode),
    InFlight(&'a Flight)
}

#[derive(Debug)]
pub struct Aircraft<'a> {
    tail: String,
    location: Location<'a>,
    type_: (String, u16)
}

