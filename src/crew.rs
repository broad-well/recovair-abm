use crate::aircraft::{Flight, Location};
use crate::model::Model;
use chrono::{DateTime, Duration, Utc};
use std::cmp::{max, min};

pub type CrewId = u32;
pub const DUTY_HOURS: i64 = 10;

#[derive(Debug)]
pub struct Crew<'a> {
    id: CrewId,
    location: Location<'a>,
    /// Ordered by time
    duty: Vec<&'a Flight<'a>>,
    model: &'a Model<'a>,
}

impl<'a> Crew<'a> {
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

    fn duty_during(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
        self.duty
            .iter()
            .rev()
            .skip_while(|flt| flt.depart_time.unwrap() >= *end)
            .take_while(|flt| flt.arrive_time.unwrap() >= *start)
            .map(|flt| duration_in_range(flt, start, end))
            .sum()
    }
}

fn duration_in_range(flight: &Flight, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Duration {
    min(&flight.arrive_time.unwrap(), end)
        .signed_duration_since(max(&flight.depart_time.unwrap(), start))
}
