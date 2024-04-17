use crate::aircraft::{Flight, FlightId, Location};
use crate::airport::AirportCode;
use crate::model::Model;
use chrono::{DateTime, Duration, TimeDelta, Utc};
use std::cmp::{max, min};

pub type CrewId = u32;
pub const DUTY_HOURS: i64 = 10;

#[derive(Debug)]
pub struct Crew {
    pub id: CrewId,
    pub location: Location,
    /// Ordered by time
    pub duty: Vec<FlightId>,
    pub next_claimed: Option<FlightId>,
}

impl Crew {
    pub fn new(id: CrewId, location: AirportCode, now: DateTime<Utc>) -> Self {
        Self {
            id,
            location: Location::Ground(location, now - TimeDelta::hours(2)),
            duty: Vec::new(),
            next_claimed: None
        }
    }

    pub fn remaining_after(&self, flight: &Flight, model: &Model) -> Duration {
        self.remaining_after_time(flight, model.now(), model)
    }

    pub fn remaining_after_time(
        &self,
        flight: &Flight,
        now: DateTime<Utc>,
        model: &Model,
    ) -> Duration {
        // formula: did we exceed 10-x hours of flight time
        // in the past 24-x hours, where x is the next flight's duration?
        let flight_duration = flight
            .sched_arrive
            .signed_duration_since(flight.sched_depart);
        let interval_start = &(now - Duration::hours(24) + flight_duration);
        let interval_end = &now;
        let duty_after = self.duty_during(interval_start, interval_end, model) + flight_duration;

        Duration::hours(DUTY_HOURS) - duty_after
    }

    pub fn takeoff(&mut self, flight: &Flight) {
        self.location = Location::InFlight(flight.id);
        if flight.crew[0] == self.id {
            self.duty.push(flight.id);
        }
        if let Some(claimed) = self.next_claimed {
            debug_assert_eq!(claimed, flight.id);
        }
        self.next_claimed = None;
    }

    pub fn land(&mut self, fl: &Flight, now: DateTime<Utc>) {
        let Location::InFlight(flight) = self.location else {
            panic!("land() called on crew when not in flight")
        };
        debug_assert_eq!(flight, fl.id);
        self.location = Location::Ground(fl.dest, now);
    }

    fn duty_during(&self, start: &DateTime<Utc>, end: &DateTime<Utc>, model: &Model) -> Duration {
        self.duty
            .iter()
            .rev()
            .skip_while(|flt| model.flight_read(**flt).depart_time.unwrap() >= *end)
            .take_while(|flt| {
                let flt = model.flight_read(**flt);
                flt.arrive_time.unwrap_or(flt.act_arrive_time()) >= *start
            })
            .map(|flt| duration_in_range(&model.flight_read(*flt), start, end))
            .sum()
    }

    pub fn time_until_available_for(
        &self,
        flight: &Flight,
        now: DateTime<Utc>,
        model: &Model,
    ) -> Option<Duration> {
        if let Some(claimer) = self.next_claimed {
            if claimer != flight.id {
                return None;
            }
        }
        let turnaround_time = model.config.crew_turnaround_time;
        match self.location {
            Location::Ground(location, since) => {
                if location != flight.origin {
                    return None;
                }
                if self.remaining_after(flight, model) < Duration::zero() {
                    return None;
                }
                let available_time = since + turnaround_time;
                Some(max(Duration::zero(), available_time - now))
            }
            Location::InFlight(ongoing) => {
                let ongoing_flt = model.flight_read(ongoing);
                if self.remaining_after_time(flight, ongoing_flt.act_arrive_time(), model)
                    < Duration::zero() || ongoing_flt.dest != flight.origin
                {
                    return None;
                }
                Some(ongoing_flt.act_arrive_time() + turnaround_time - now)
            }
        }
    }

    pub fn claim(&mut self, flight: FlightId) {
        debug_assert!(self.next_claimed.is_none());
        self.next_claimed = Some(flight);
    }

    pub fn unclaim(&mut self, flight: FlightId) {
        if self.next_claimed == Some(flight) {
            self.next_claimed = None;
        }
    }
}

fn duration_in_range(flight: &Flight, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
    min(&flight.arrive_time.unwrap_or(flight.act_arrive_time()), end)
        .signed_duration_since(max(&flight.depart_time.unwrap(), start))
}
