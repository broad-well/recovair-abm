use crate::aircraft::{Flight, FlightId, Location};
use crate::model::Model;
use chrono::{DateTime, Duration, Utc};
use std::cmp::{max, min};
use std::rc::Rc;

pub type CrewId = u32;
pub const DUTY_HOURS: i64 = 10;

#[derive(Debug)]
pub struct Crew {
    pub id: CrewId,
    location: Location,
    /// Ordered by time
    duty: Vec<FlightId>,
    model: Rc<Model>,
}

impl Crew {
    pub fn remaining_after(&self, flight: &Flight) -> Duration {
        // formula: did we exceed 10-x hours of flight time
        // in the past 24-x hours, where x is the next flight's duration?
        let flight_duration = flight
            .arrive_time
            .unwrap()
            .signed_duration_since(flight.depart_time.unwrap());
        let interval_start = &(self.model.now - Duration::hours(24) + flight_duration);
        let interval_end = &self.model.now;
        let duty_after = self.duty_during(interval_start, interval_end) + flight_duration;

        max(Duration::zero(), Duration::hours(DUTY_HOURS) - duty_after)
    }

    pub fn takeoff(&mut self, flight: FlightId) {
        self.location = Location::InFlight(flight);
    }

    pub fn land(&mut self) {
        let Location::InFlight(flight) = self.location else {
            panic!("land() called on crew when not in flight")
        };
        let fl = self.model.flight_read(flight);
        if fl.crew[0] == self.id {
            self.duty.push(flight);
        }
        self.location = Location::Ground(fl.dest, self.model.now);
    }

    fn duty_during(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
        self.duty
            .iter()
            .rev()
            .map(|id| self.model.flight_read(*id))
            .skip_while(|flt| flt.depart_time.unwrap() >= *end)
            .take_while(|flt| flt.arrive_time.unwrap() >= *start)
            .map(|flt| duration_in_range(&flt, start, end))
            .sum()
    }
}

fn duration_in_range(flight: &Flight, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
    min(&flight.arrive_time.unwrap(), end)
        .signed_duration_since(max(&flight.depart_time.unwrap(), start))
}
