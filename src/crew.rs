use crate::aircraft::{Flight, FlightId, Location};
use crate::airport::AirportCode;
use crate::model::Model;
use chrono::{DateTime, Duration, Utc};
use std::cmp::{max, min};
use std::rc::{Rc, Weak};
use std::sync::RwLock;

pub type CrewId = u32;
pub const DUTY_HOURS: i64 = 10;

#[derive(Debug)]
pub struct Crew {
    pub id: CrewId,
    location: Location,
    /// Ordered by time
    duty: Vec<FlightId>,
    model: Weak<RwLock<Model>>,
}

impl Crew {
    pub fn remaining_after(&self, flight: &Flight) -> Duration {
        self.remaining_after_time(flight, self.model.upgrade().unwrap().read().unwrap().now)
    }

    pub fn remaining_after_time(&self, flight: &Flight, now: DateTime<Utc>) -> Duration {
        // formula: did we exceed 10-x hours of flight time
        // in the past 24-x hours, where x is the next flight's duration?
        let flight_duration = flight
            .arrive_time
            .unwrap()
            .signed_duration_since(flight.depart_time.unwrap());
        let interval_start = &(now - Duration::hours(24) + flight_duration);
        let interval_end = &now;
        let duty_after = self.duty_during(interval_start, interval_end) + flight_duration;

        Duration::hours(DUTY_HOURS) - duty_after
    }

    pub fn takeoff(&mut self, flight: &Flight) {
        self.location = Location::InFlight(flight.id);
        if flight.crew[0] == self.id {
            self.duty.push(flight.id);
        }
    }

    pub fn land(&mut self, fl: &Flight, now: DateTime<Utc>) {
        let Location::InFlight(flight) = self.location else {
            panic!("land() called on crew when not in flight")
        };
        self.location = Location::Ground(fl.dest, now);
    }

    /// Acquires a Read lock
    fn duty_during(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
        self.duty
            .iter()
            .rev()
            .skip_while(|flt| {
                self.model
                    .upgrade()
                    .unwrap()
                    .read()
                    .unwrap()
                    .flight_read(**flt)
                    .depart_time
                    .unwrap()
                    >= *end
            })
            .take_while(|flt| {
                self.model
                    .upgrade()
                    .unwrap()
                    .read()
                    .unwrap()
                    .flight_read(**flt)
                    .arrive_time
                    .unwrap()
                    >= *start
            })
            .map(|flt| {
                duration_in_range(
                    &self
                        .model
                        .upgrade()
                        .unwrap()
                        .read()
                        .unwrap()
                        .flight_read(*flt),
                    start,
                    end,
                )
            })
            .sum()
    }

    pub fn time_until_available_for(
        &self,
        flight: &Flight,
        now: DateTime<Utc>,
    ) -> Option<Duration> {
        let turnaround_time = self
            .model
            .upgrade()
            .unwrap()
            .read()
            .unwrap()
            .config
            .crew_turnaround_time;
        match self.location {
            Location::Ground(location, since) => {
                if location != flight.origin {
                    return None;
                }
                if self.remaining_after(flight) < Duration::zero() {
                    return None;
                }
                let available_time = since + turnaround_time;
                Some(max(Duration::zero(), available_time - now))
            }
            Location::InFlight(ongoing) => {
                let ptr = self.model.upgrade().unwrap();
                let mdl = ptr.read().unwrap();
                let ongoing_flt = mdl.flight_read(ongoing);
                if self.remaining_after_time(flight, ongoing_flt.act_arrive_time())
                    < Duration::zero()
                {
                    return None;
                }
                Some(ongoing_flt.act_arrive_time() + turnaround_time - now)
            }
        }
    }
}

fn duration_in_range(flight: &Flight, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
    min(&flight.arrive_time.unwrap(), end)
        .signed_duration_since(max(&flight.depart_time.unwrap(), start))
}
