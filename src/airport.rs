use std::collections::LinkedList;
use chrono::{DateTime, Days, TimeDelta, Utc};

use crate::{aircraft::{Aircraft, Flight}, crew::Crew, model::Model};

pub type AirportCode = [u8; 3];
#[derive(Debug)]
pub struct Airport<'a> {
    pub code: AirportCode,
    pub model: &'a Model<'a>,
    pub fleet: LinkedList<&'a Aircraft<'a>>,
    pub crew: LinkedList<&'a Crew<'a>>,
    pub passengers: LinkedList<PassengerGroup<'a>>
}

pub struct PassengerDemand {
    pub path: Vec<AirportCode>,
    pub count: u32
}

/// Group of passengers transported
/// 
/// Owner: Airport or Flight
#[derive(Debug)]
pub struct PassengerGroup<'a> {
    pub path: &'a [AirportCode],
    pub count: u32
}

// MARK: Disruptions

pub enum Clearance {
    Cleared,
    /// Unlikely to be delayed further
    EDCT(DateTime<Utc>),
    /// Delay, no implication on likelihood to be delayed further
    Deferred(DateTime<Utc>)
}

pub trait Disruption<'a>: std::fmt::Debug {
    /// Mutability for tracking slots internally
    /// 
    /// By design, we should call this AFTER ensuring that all the resources are present for the flight
    /// (aircraft, crew, passengers)
    fn request_depart(&mut self, flight: &'a Flight<'a>) -> Clearance;

    fn describe(&self) -> String;
}

#[derive(Debug)]
pub struct SlotManager<T: PartialEq> {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    slots_assigned: Vec<Vec<T>>,
    max_slot_size: u16
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
        } else { None }
    }

    pub fn contains(&self, time: &DateTime<Utc>) -> bool {
        *time >= self.start && *time <= self.end
    }

    fn time_to_index(&self, time: &DateTime<Utc>) -> usize {
        (*time - self.start).num_hours() as usize
    }

    fn slot_time_estimate(&self, i: usize, si: usize) -> DateTime<Utc> {
        self.start + TimeDelta::hours(i as i64) +
            TimeDelta::minutes(((si as f32) / (self.max_slot_size as f32) * 60f32).floor() as i64)
    }
}

#[derive(Debug)]
pub struct GroundDelayProgram<'a> {
    pub site: AirportCode,
    // Room to add origin ARTCCs
    pub slots: SlotManager<&'a Flight<'a>>,
    pub reason: Option<String>,
    model: &'a Model<'a>
}

impl<'a> GroundDelayProgram<'a> {
    #[inline]
    pub fn start(&self) -> &DateTime<Utc> {
        &self.slots.start
    }
    #[inline]
    pub fn end(&self) -> &DateTime<Utc> {
        &self.slots.end
    }
}

impl<'a> Disruption<'a> for GroundDelayProgram<'a> {
    fn request_depart(&mut self, flight: &'a Flight<'a>) -> Clearance {
        if flight.dest != self.site {
            return Clearance::Cleared;
        }
        let arrive = flight.est_arrive_time(&self.model.now);
        if !self.slots.contains(&arrive) {
            return Clearance::Cleared;
        }
        // find the next available slot (first come first serve)
        // this generally works in favor of reducing delays relative to scheduled departure time
        // because earlier scheduled flights request departure before later ones
        // if already given a slot in the currently scheduled arrive time, then clear it
        if self.slots.slotted_at(&arrive, &flight) {
            Clearance::Cleared
        } else if let Some(edct) = self.slots.allocate_slot(&arrive, flight) {
            Clearance::EDCT(edct)
        } else {
            // Can't fit during this GDP. check later
            Clearance::Deferred(self.end().clone())
        }
    }

    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!("Ground delay program at {} from {} to {} due to {}", String::from_utf8(self.site.into()).unwrap(), self.start(), self.end(), reason)
        } else {
            format!("Ground delay program at {} from {} to {}", String::from_utf8(self.site.into()).unwrap(), self.start(), self.end())
        }
    }
}

#[derive(Debug)]
pub struct DepartureRateLimit<'a> {
    site: AirportCode,
    slots: SlotManager<&'a Flight<'a>>,
    reason: Option<String>,
    model: &'a Model<'a>
}

impl<'a> Disruption<'a> for DepartureRateLimit<'a> {
    fn request_depart(&mut self, flight: &'a Flight<'a>) -> Clearance {
        if flight.origin != self.site { return Clearance::Cleared; }
        if !self.slots.contains(&self.model.now) { return Clearance::Cleared; }

        if self.slots.slotted_at(&self.model.now, &flight) {
            Clearance::Cleared
        } else if let Some(edct) = self.slots.allocate_slot(&self.model.now, flight) {
            Clearance::EDCT(edct)
        } else {
            Clearance::Deferred(self.slots.end.clone())
        }
    }
    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!("Departure delay program at {} from {} to {} due to {}", String::from_utf8(self.site.into()).unwrap(), self.slots.start, self.slots.end, reason)
        } else {
            format!("Departure delay program at {} from {} to {}", String::from_utf8(self.site.into()).unwrap(), self.slots.start, self.slots.end)
        }
    }
}
