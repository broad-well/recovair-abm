use crate::airport::*;
use chrono::{DateTime, Utc};
use crate::crew::CrewId;

/// Represents a flight (either planned or executed).
/// If `arrive_time` is None, then this must be a flight in progress.
///
/// Owner: Model
#[derive(Debug)]
pub struct Flight<'a> {
    pub flight_number: String,
    pub aircraft_tail: String,
    /// First element is piloting, the rest are deadheading
    pub crew: Vec<CrewId>,
    /// If empty, then this is a ferry flight
    pub passengers: Vec<PassengerGroup<'a>>,
    pub origin: AirportCode,
    pub dest: AirportCode,
    pub depart_time: Option<DateTime<Utc>>,
    pub arrive_time: Option<DateTime<Utc>>,

    pub sched_depart: DateTime<Utc>,
    pub sched_arrive: DateTime<Utc>
}

impl<'a> PartialEq for Flight<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.flight_number == other.flight_number
    }
}

impl<'a> Flight<'a> {
    #[inline]
    pub fn in_flight(&self) -> bool {
        self.arrive_time.is_none()
    }

    pub fn est_arrive_time(&self, depart: &DateTime<Utc>) -> DateTime<Utc> {
        *depart + (self.sched_arrive - self.sched_depart)
    }
}

/// Owner: Aircraft or Crew
#[derive(Debug)]
pub enum Location<'a> {
    Ground(AirportCode),
    InFlight(&'a Flight<'a>)
}

#[derive(Debug)]
pub struct Aircraft<'a> {
    tail: String,
    location: Location<'a>,
    /// (Name, passenger capacity)
    type_: (String, u16),
}

