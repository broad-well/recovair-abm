use crate::crew::CrewId;
use crate::{airport::*, metrics::ModelEventType};
use chrono::{DateTime, Utc};

pub type FlightId = u64;

/// Represents a flight (either planned or executed).
/// If `arrive_time` is None, then this must be a flight in progress.
///
/// Owner: Model
#[derive(Debug)]
pub struct Flight {
    pub id: FlightId,
    pub flight_number: String,
    pub aircraft_tail: String,
    /// First element is piloting, the rest are deadheading
    pub crew: Vec<CrewId>,
    /// If empty, then this is a ferry flight
    pub passengers: Vec<PassengerDemand>,
    pub origin: AirportCode,
    pub dest: AirportCode,
    pub depart_time: Option<DateTime<Utc>>,
    pub arrive_time: Option<DateTime<Utc>>,

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
    pub fn in_flight(&self) -> bool {
        self.depart_time.is_some() && self.arrive_time.is_none()
    }

    #[inline]
    pub fn is_ferry_flight(&self) -> bool {
        self.passengers.is_empty()
    }

    pub fn est_arrive_time(&self, depart: &DateTime<Utc>) -> DateTime<Utc> {
        *depart + (self.sched_arrive - self.sched_depart)
    }

    pub fn act_arrive_time(&self) -> DateTime<Utc> {
        self.est_arrive_time(
            &self
                .depart_time
                .expect("Don't call act_arrive_time on a flight that has not departed"),
        )
    }

    pub fn takeoff(&mut self, time: DateTime<Utc>) {
        self.depart_time = Some(time);
    }

    pub fn land(&mut self, time: DateTime<Utc>) {
        self.arrive_time = Some(time);
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
    tail: String,
    location: Location,
    /// (Name, passenger capacity)
    type_: (String, u16),
}

impl Aircraft {
    pub fn takeoff(&mut self, flight_id: FlightId, time: DateTime<Utc>) -> ModelEventType {
        let Location::Ground(airport, since) = self.location else {
            panic!("takeoff() called when aircraft in the air")
        };
        self.location = Location::InFlight(flight_id);
        ModelEventType::AircraftTurnedAround(self.tail.clone(), airport, time - since)
    }

    pub fn land(&mut self, loc: AirportCode, time: DateTime<Utc>) {
        self.location = Location::Ground(loc, time);
    }
}
