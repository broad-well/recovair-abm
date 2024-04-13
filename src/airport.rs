use chrono::{DateTime, TimeDelta, Utc};
use std::{
    cmp::Ordering,
    collections::LinkedList,
    rc::Weak,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use crate::{
    aircraft::{Aircraft, Flight, FlightId},
    crew::{Crew, CrewId},
    model::Model,
};

pub type AirportCode = [u8; 3];
#[derive(Debug)]
pub struct Airport {
    pub code: AirportCode,
    pub fleet: LinkedList<String>,
    pub crew: LinkedList<CrewId>,
    // TODO consolidate demand path
    pub passengers: LinkedList<PassengerDemand>,

    pub max_dep_per_hour: u32,
    pub max_arr_per_hour: u32,
    departure_count: (DateTime<Utc>, u32),
    arrival_count: (DateTime<Utc>, u32),
}

impl Airport {
    pub fn depart_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        if time - self.departure_count.0 >= TimeDelta::hours(1) {
            // Seems like we need to reset the counter
            time
        } else if self.departure_count.1 < self.max_dep_per_hour {
            // We can fit it in
            time
        } else {
            // Delayed to the next slot
            self.departure_count.0 + TimeDelta::minutes(60)
        }
    }

    pub fn mark_departure(&mut self, time: DateTime<Utc>) {
        if time - self.departure_count.0 >= TimeDelta::hours(1) {
            self.departure_count = (time, 1);
        } else {
            self.departure_count.1 += 1
        }
    }

    // TODO reduce duplication
    pub fn arrive_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        if time - self.arrival_count.0 >= TimeDelta::hours(1) {
            // Seems like we need to reset the counter
            time
        } else if self.arrival_count.1 < self.max_arr_per_hour {
            // We can fit it in
            time
        } else {
            // Delayed to the next slot
            self.arrival_count.0 + TimeDelta::minutes(60)
        }
    }

    pub fn mark_arrival(&mut self, time: DateTime<Utc>) {
        if time - self.arrival_count.0 >= TimeDelta::hours(1) {
            self.arrival_count = (time, 1);
        } else {
            self.arrival_count.1 += 1
        }
    }
}

#[derive(Debug)]
pub struct PassengerDemand {
    pub path: Vec<AirportCode>,
    pub count: u32,
}

// MARK: Disruptions

#[derive(PartialEq, Eq, Debug)]
pub enum Clearance {
    Cleared,
    /// Unlikely to be delayed further
    #[allow(clippy::upper_case_acronyms)]
    EDCT(DateTime<Utc>),
    /// Delay, no implication on likelihood to be delayed further
    Deferred(DateTime<Utc>),
}

impl Clearance {
    pub fn time(&self) -> Option<&DateTime<Utc>> {
        match self {
            Self::Cleared => None,
            Self::EDCT(dt) => Some(dt),
            Self::Deferred(dt) => Some(dt),
        }
    }
}

impl Ord for Clearance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self == other {
            Ordering::Equal
        } else if matches!(self, Clearance::Cleared) {
            Ordering::Less
        } else if matches!(other, Clearance::Cleared) {
            Ordering::Greater
        } else {
            let self_time = self.time().unwrap();
            let other_time = other.time().unwrap();
            if self_time != other_time {
                self_time.cmp(other_time)
            } else if matches!(self, Clearance::EDCT(_)) {
                // !=, none cleared, time the same
                assert!(matches!(other, Clearance::Deferred(_)));
                Ordering::Less
            } else {
                assert!(matches!(other, Clearance::EDCT(_)));
                Ordering::Greater
            }
        }
    }
}

impl PartialOrd for Clearance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait Disruption: std::fmt::Debug {
    /// By design, we should call this AFTER ensuring that all the resources are present for the flight
    /// (aircraft, crew, passengers)
    fn request_depart(&mut self, flight: RwLockReadGuard<'_, Flight>) -> Clearance;
    fn request_arrive(&mut self, _flight: RwLockReadGuard<'_, Flight>) -> Clearance {
        Clearance::Cleared
    }

    fn describe(&self) -> String;
}

#[derive(Debug)]
pub struct SlotManager<T: PartialEq> {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    slots_assigned: Vec<Vec<T>>,
    max_slot_size: u16,
}

impl<T: PartialEq> SlotManager<T> {
    pub fn slotted_at(&self, time: &DateTime<Utc>, item: &T) -> bool {
        self.slots_assigned[self.time_to_index(time)].contains(item)
    }
    pub fn allocate_slot(&mut self, start_time: &DateTime<Utc>, item: T) -> Option<DateTime<Utc>> {
        let index_start = self.time_to_index(start_time);

        let mut first_open: Option<usize> = None;
        for i in index_start..self.slots_assigned.len() {
            let slots = &self.slots_assigned[i];
            // Existing slot found?
            if let Some((si, _)) = slots.iter().enumerate().find(|(_, s)| *s == &item) {
                return Some(self.slot_time_estimate(i, si));
            }
            if first_open.is_none() && slots.len() < self.max_slot_size as usize {
                first_open = Some(i);
            }
        }
        // New slot
        if let Some(first_open) = first_open {
            self.slots_assigned[first_open].push(item);
            Some(self.slot_time_estimate(first_open, self.slots_assigned[first_open].len() - 1))
        } else {
            None
        }
    }

    pub fn contains(&self, time: &DateTime<Utc>) -> bool {
        *time >= self.start && *time <= self.end
    }

    fn time_to_index(&self, time: &DateTime<Utc>) -> usize {
        (*time - self.start).num_hours() as usize
    }

    fn slot_time_estimate(&self, i: usize, si: usize) -> DateTime<Utc> {
        self.start
            + TimeDelta::hours(i as i64)
            + TimeDelta::minutes(((si as f32) / (self.max_slot_size as f32) * 60f32).floor() as i64)
    }
}

#[derive(Debug)]
pub struct GroundDelayProgram {
    pub site: AirportCode,
    // Room to add origin ARTCCs
    pub slots: SlotManager<FlightId>,
    pub reason: Option<String>,
    model: Weak<Model>,
}

impl GroundDelayProgram {
    #[inline]
    pub fn start(&self) -> &DateTime<Utc> {
        &self.slots.start
    }
    #[inline]
    pub fn end(&self) -> &DateTime<Utc> {
        &self.slots.end
    }
}

impl Disruption for GroundDelayProgram {
    fn request_depart(&mut self, flight: RwLockReadGuard<'_, Flight>) -> Clearance {
        if flight.dest != self.site {
            return Clearance::Cleared;
        }
        let arrive = flight.est_arrive_time(&self.model.upgrade().unwrap().now);
        if !self.slots.contains(&arrive) {
            return Clearance::Cleared;
        }
        // find the next available slot (first come first serve)
        // this generally works in favor of reducing delays relative to scheduled departure time
        // because earlier scheduled flights request departure before later ones
        // if already given a slot in the currently scheduled arrive time, then clear it
        if self.slots.slotted_at(&arrive, &flight.id) {
            Clearance::Cleared
        } else if let Some(edct) = self.slots.allocate_slot(&arrive, flight.id) {
            Clearance::EDCT(edct)
        } else {
            // Can't fit during this GDP. check later
            Clearance::Deferred(*self.end())
        }
    }

    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!(
                "Ground delay program at {} from {} to {} due to {}",
                String::from_utf8(self.site.into()).unwrap(),
                self.start(),
                self.end(),
                reason
            )
        } else {
            format!(
                "Ground delay program at {} from {} to {}",
                String::from_utf8(self.site.into()).unwrap(),
                self.start(),
                self.end()
            )
        }
    }
}

#[derive(Debug)]
pub struct DepartureRateLimit {
    site: AirportCode,
    slots: SlotManager<FlightId>,
    reason: Option<String>,
    model: Arc<Model>,
}

impl Disruption for DepartureRateLimit {
    fn request_depart(&mut self, flight: RwLockReadGuard<'_, Flight>) -> Clearance {
        if flight.origin != self.site {
            return Clearance::Cleared;
        }
        if !self.slots.contains(&self.model.now) {
            return Clearance::Cleared;
        }

        if self.slots.slotted_at(&self.model.now, &flight.id) {
            Clearance::Cleared
        } else if let Some(edct) = self.slots.allocate_slot(&self.model.now, flight.id) {
            Clearance::EDCT(edct)
        } else {
            Clearance::Deferred(self.slots.end)
        }
    }
    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!(
                "Departure delay program at {} from {} to {} due to {}",
                String::from_utf8(self.site.into()).unwrap(),
                self.slots.start,
                self.slots.end,
                reason
            )
        } else {
            format!(
                "Departure delay program at {} from {} to {}",
                String::from_utf8(self.site.into()).unwrap(),
                self.slots.start,
                self.slots.end
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clearance_comp_different_time() {
        let mut c1 = Clearance::Cleared;
        let now = Utc::now();
        let mut c2 = Clearance::EDCT(now);

        assert!(c1 < c2);
        c2 = Clearance::Deferred(now);
        assert!(c1 < c2);

        c1 = Clearance::EDCT(now + TimeDelta::minutes(2));
        assert!(c1 > c2);

        c2 = Clearance::EDCT(now + TimeDelta::minutes(10));
        assert!(c1 < c2);

        c1 = Clearance::Deferred(now);
        c2 = Clearance::Deferred(now + TimeDelta::minutes(20));
        assert!(c1 < c2);
        assert!(c2 > c1);
    }

    #[test]
    fn clearance_comp_same_time() {
        let now = Utc::now();
        assert_eq!(Clearance::EDCT(now), Clearance::EDCT(now));
        assert_eq!(Clearance::Deferred(now), Clearance::Deferred(now));
        assert!(Clearance::EDCT(now) < Clearance::Deferred(now));
        assert!(Clearance::Deferred(now) > Clearance::EDCT(now));
    }
}
