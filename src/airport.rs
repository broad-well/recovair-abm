use chrono::{DateTime, TimeDelta, Utc};
use std::{
    cmp::{min, Ordering},
    collections::{HashMap, HashSet},
    fmt::Debug,
    iter::{empty, repeat, repeat_with, Repeat},
    sync::{Arc, RwLock},
};

use crate::{
    aircraft::{Flight, FlightId},
    crew::CrewId,
    model::Model,
};

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd)]
pub struct AirportCode {
    letters: [u8; 3],
}

impl AirportCode {
    pub fn from(string: &String) -> Self {
        let mut ac = Self { letters: [b'A'; 3] };
        ac.letters.clone_from_slice(string.as_bytes());
        ac
    }
}

impl std::fmt::Debug for AirportCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.letters[0] as char, self.letters[1] as char, self.letters[2] as char
        )?;
        Ok(())
    }
}

impl std::fmt::Display for AirportCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

#[derive(Debug)]
pub struct Airport {
    pub code: AirportCode,
    pub fleet: HashSet<String>,
    pub crew: HashSet<CrewId>,
    // TODO consolidate demand path
    pub passengers: Vec<PassengerDemand>,

    pub max_dep_per_hour: u32,
    pub max_arr_per_hour: u32,
    pub departure_count: (DateTime<Utc>, u32),
    pub arrival_count: (DateTime<Utc>, u32),
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

    /// Precondition: The given flight has been assigned to an aircraft
    pub fn mark_departure(&mut self, time: DateTime<Utc>, flight: &mut Flight, capacity: u16) {
        if time - self.departure_count.0 >= TimeDelta::hours(1) {
            self.departure_count = (time, 1);
        } else {
            self.departure_count.1 += 1;
        }
        debug_assert!(self.fleet.remove(flight.aircraft_tail.as_ref().unwrap()));
        self.crew.retain(|c| !flight.crew.contains(c));
        self.deduct_passengers(flight.id, flight.dest, capacity, &mut flight.passengers);
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

    pub fn mark_arrival(&mut self, time: DateTime<Utc>, flight: &Flight) {
        if time - self.arrival_count.0 >= TimeDelta::hours(1) {
            self.arrival_count = (time, 1);
        } else {
            self.arrival_count.1 += 1;
        }
        self.fleet.insert(flight.aircraft_tail.clone().unwrap());
        self.crew.extend(flight.crew.iter());
        self.accept_passengers(&flight.passengers);
    }

    fn deduct_passengers(
        &mut self,
        flight: FlightId,
        dest: AirportCode,
        capacity: u16,
        onboard: &mut Vec<PassengerDemand>,
    ) {
        let mut capacity = capacity as i32;
        // TODO figure out which ones to prioritize
        for demand in &mut self.passengers {
            if capacity <= 0 {
                break;
            }
            if demand.next_dest(self.code) != Some(dest) {
                continue;
            }
            let taking = min(demand.count, capacity as u32);
            // if taking == 0 {
            //     println!("{} {}", demand.count, capacity);
            // }
            onboard.push(demand.split_off(taking, flight));
            capacity -= taking as i32;
        }
        self.passengers.retain(|demand| demand.count > 0);
    }

    fn accept_passengers(&mut self, onboard: &[PassengerDemand]) {
        self.passengers.extend(
            onboard
                .iter()
                .filter(|demand| *demand.path.last().unwrap() != self.code)
                .map(|d| d.clone()),
        );
    }
}

#[derive(Debug, Clone)]
pub struct PassengerDemand {
    pub path: Vec<AirportCode>,
    pub count: u32,
    pub flights_taken: Vec<FlightId>,
}

impl PassengerDemand {
    pub fn next_dest(&self, now: AirportCode) -> Option<AirportCode> {
        self.path
            .iter()
            .skip_while(|code| **code != now)
            .nth(1)
            .map(|i| *i)
    }

    pub fn split_off(&mut self, count: u32, flight: FlightId) -> Self {
        assert!(count > 0);
        self.count -= count;
        Self {
            path: self.path.clone(),
            count,
            flights_taken: {
                let mut copy = Vec::with_capacity(self.flights_taken.len() + 1);
                copy.extend(self.flights_taken.iter());
                copy.push(flight);
                copy
            },
        }
    }
}

// MARK: Disruptions

#[derive(PartialEq, Eq, Debug)]
/// An outcome of requesting clearance to depart or arrive.
/// If delayed, it should include a time at which the dispatcher should request clearance again.
/// Note that the time must apply to the action being requested (i.e., GDPs should issue clearances by departure time even though it allocates slots by arrival time)
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

    #[inline]
    pub fn cleared_at(&self, time: &DateTime<Utc>) -> bool {
        self.time().map_or(true, |t| t <= time)
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
                debug_assert!(matches!(other, Clearance::Deferred(_)));
                Ordering::Less
            } else {
                debug_assert!(matches!(other, Clearance::EDCT(_)));
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

pub trait Disruption: std::fmt::Debug + Send + Sync {
    /// By design, we should call this AFTER ensuring that all the resources are present for the flight
    /// (aircraft, crew, passengers)
    fn request_depart(&mut self, flight: &Flight, model: &Model, time: &DateTime<Utc>)
        -> Clearance;
    fn request_arrive(
        &mut self,
        _flight: &Flight,
        _model: &Model,
        _time: &DateTime<Utc>,
    ) -> Clearance {
        Clearance::Cleared
    }
    fn void_depart_clearance(&mut self, flight: &Flight, time: &DateTime<Utc>, model: &Model);
    fn void_arrive_clearance(&mut self, _flight: &Flight, _time: &DateTime<Utc>, _model: &Model) {}

    fn describe(&self) -> String;

    fn departure_airports_affected(&self) -> Vec<AirportCode>;
    fn arrival_airports_affected(&self) -> Vec<AirportCode>;
}

#[derive(Debug)]
pub struct SlotManager<T: PartialEq + Debug> {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    slots_assigned: Vec<Vec<T>>,
    max_slot_size: u16,
}

impl<T: PartialEq + Debug> SlotManager<T> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, hourly_rate: u16) -> Self {
        let num_slots = (end - start).num_hours();
        let slots_assigned: Vec<Vec<T>> = std::iter::repeat_with(|| Vec::new())
            .take(num_slots as usize)
            .collect();

        Self {
            start,
            end,
            slots_assigned,
            max_slot_size: hourly_rate,
        }
    }

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

    pub fn drop_slot(&mut self, time: &DateTime<Utc>, item: T) -> bool {
        let index = self.time_to_index(time);
        if let Some(pos) = self.slots_assigned[index].iter().position(|i| *i == item) {
            self.slots_assigned[index].remove(pos);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, time: &DateTime<Utc>) -> bool {
        *time >= self.start && *time < self.end
    }

    fn time_to_index(&self, time: &DateTime<Utc>) -> usize {
        (*time - self.start).num_hours() as usize
    }

    fn slot_time_estimate(&self, i: usize, si: usize) -> DateTime<Utc> {
        let result = self.start
            + TimeDelta::hours(i as i64)
            // + TimeDelta::minutes(((si as f32) / (self.max_slot_size as f32) * 60f32).floor() as i64);
            + TimeDelta::minutes((si as f32 * (60f32 / self.max_slot_size as f32).min(3f32)).round() as i64);
        debug_assert!(result >= self.start);
        debug_assert!(
            result <= self.end,
            "result={:?}, end={:?}, i={} si={} slots_assigned has {} item(s)",
            result,
            self.end,
            i,
            si,
            self.slots_assigned.len()
        );
        result
    }
}

#[derive(Debug)]
pub struct CumulativeSmallSlotManager<T: PartialEq> {
    pub start: DateTime<Utc>,
    pub hourly_accumulation_limit: Vec<u32>,
    pub slots_assigned: RwLock<Vec<Vec<T>>>,
}

macro_rules! prefix_sum {
    ( $vec:expr ) => {
        $vec.scan(0, |sum, x| { *sum += x; Some(*sum) })
    }
}

impl<T: PartialEq> CumulativeSmallSlotManager<T> {
    const HOUR_SLACK: u32 = 3;
    const SLOT_DURATION: TimeDelta = TimeDelta::minutes(4);

    pub fn new(start: DateTime<Utc>, throughput: Vec<u32>) -> Self {
        Self {
            start,
            slots_assigned: RwLock::new(repeat_with(Vec::new)
                .take(throughput.len())
                .collect()),
            hourly_accumulation_limit: prefix_sum!(throughput.into_iter()).collect()
        }
    }

    pub fn allocate_slot(&self, query_time: &DateTime<Utc>, item: T) -> Option<DateTime<Utc>> {
        let query_index = (*query_time - self.start).num_hours() as u32;
        // Need to maintain exclusive write access to slots until after they are mutated
        let mut slots = self.slots_assigned.write().unwrap();
        let accum = self.assigned_accumulation(&slots);
        let first_with_capacity = std::iter::zip(accum.iter(), self.hourly_accumulation_limit.iter())
            .enumerate()
            .rev()
            .skip_while(|&(_, (current, limit))| *current < *limit)
            .next()
            .map(|pair| pair.0 + 1);

        if first_with_capacity.map(|index| index >= slots.len()) == Some(true) {
            // all full
            return None;
        }

        let first_ok_index = accum.into_iter()
            .enumerate()
            .skip(std::cmp::max(query_index as usize, first_with_capacity.unwrap_or(0)))
            .find(|&(i, assigned_accum)| {
                let slot_limit = self.expected_throughput(i) + Self::HOUR_SLACK;
                assigned_accum < self.hourly_accumulation_limit[i] && slots[i].len() < slot_limit as usize
            })
            .map(|i| i.0);

        if let Some(index) = first_ok_index {
            let slot_ordinal = slots[index].len();
            slots[index].push(item);
            let time_estimate = self.slot_size(index) * slot_ordinal as i32;
            Some(self.start + TimeDelta::hours(index as i64) + time_estimate)
        } else { None }
    }

    #[inline]
    fn expected_throughput(&self, i: usize) -> u32 {
        self.hourly_accumulation_limit[i] -
            if i == 0 {
                0
            } else {
                self.hourly_accumulation_limit[i - 1]
            }
    }

    #[inline]
    fn slot_size(&self, i: usize) -> TimeDelta {
        std::cmp::min(TimeDelta::hours(1) / self.expected_throughput(i) as i32, Self::SLOT_DURATION)
    }

    fn assigned_accumulation(&self, slots: &Vec<Vec<T>>) -> Vec<u32> {
        Box::new(prefix_sum!(Box::new(slots.iter().map(Vec::len))
            .map(|x| x as u32)))
            .collect()
    }
}

#[derive(Debug)]
pub struct GroundDelayProgram {
    pub site: AirportCode,
    // Room to add origin ARTCCs
    pub slots: SlotManager<FlightId>,
    pub reason: Option<String>,
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
    fn request_depart(
        &mut self,
        flight: &Flight,
        model: &Model,
        time: &DateTime<Utc>,
    ) -> Clearance {
        if flight.dest != self.site {
            return Clearance::Cleared;
        }
        let arrive = flight.est_arrive_time(time);
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
            // println!("{} slotted {} at EDCT {:?}", self.describe(), flight.id, edct);
            Clearance::EDCT(std::cmp::max(model.now(), edct) - flight.est_duration())
        } else {
            // Can't fit during this GDP. check later
            // println!("{} REJECTED {} (slots = {:?})", self.describe(), flight.id, self.slots.slots_assigned);
            Clearance::Deferred(*self.end() - flight.est_duration())
        }
    }

    fn void_depart_clearance(&mut self, flight: &Flight, time: &DateTime<Utc>, _model: &Model) {
        let slot_time = flight.est_arrive_time(time);
        if self.slots.contains(&slot_time) {
            // println!("{} VOIDED departure clearance for flight {} at {:?} (slots used to be {:?})", self.describe(), flight.id, time, self.slots.slots_assigned);
            self.slots.drop_slot(&slot_time, flight.id);
            // println!("^^ slots are now {:?}", self.slots.slots_assigned);
        }
    }

    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!(
                "Ground delay program at {} from {} to {} (flights arrive at a rate of {} per hour) due to {}",
                self.site,
                self.start(),
                self.end(),
                self.slots.max_slot_size,
                reason
            )
        } else {
            format!(
                "Ground delay program at {} from {} to {} (flights arrive at a rate of {} per hour)",
                self.site,
                self.start(),
                self.end(),
                self.slots.max_slot_size,
            )
        }
    }

    fn arrival_airports_affected(&self) -> Vec<AirportCode> {
        vec![self.site]
    }

    fn departure_airports_affected(&self) -> Vec<AirportCode> {
        Vec::new()
    }
}

#[derive(Debug)]
pub struct DepartureRateLimit {
    pub site: AirportCode,
    pub slots: SlotManager<FlightId>,
    pub reason: Option<String>,
}

impl Disruption for DepartureRateLimit {
    fn request_depart(
        &mut self,
        flight: &Flight,
        model: &Model,
        time: &DateTime<Utc>,
    ) -> Clearance {
        if flight.origin != self.site {
            return Clearance::Cleared;
        }
        if !self.slots.contains(time) {
            return Clearance::Cleared;
        }

        if self.slots.slotted_at(time, &flight.id) {
            Clearance::Cleared
        } else if let Some(edct) = self.slots.allocate_slot(time, flight.id) {
            // println!("{} slotted {} at EDCT {:?}", self.describe(), flight.id, edct);
            if edct <= model.now() {
                Clearance::Cleared
            } else {
                Clearance::EDCT(std::cmp::max(model.now(), edct))
            }
        } else {
            // println!("{} REJECTED {} (slots = {:?})", self.describe(), flight.id, self.slots.slots_assigned);
            if self.slots.end == model.now() {
                Clearance::Cleared
            } else {
                Clearance::Deferred(self.slots.end)
            }
        }
    }

    fn void_depart_clearance(&mut self, flight: &Flight, time: &DateTime<Utc>, _model: &Model) {
        if self.slots.contains(time) {
            // println!("{} VOIDED departure clearance for flight {} at {:?} (slots used to be {:?})", self.describe(), flight.id, time, self.slots.slots_assigned);
            self.slots.drop_slot(time, flight.id);
            // println!("^^ slots are now {:?}", self.slots.slots_assigned);
        }
    }

    fn describe(&self) -> String {
        if let Some(reason) = &self.reason {
            format!(
                "Departure delay program at {} from {} to {} (flights depart at a rate of {} per hour) due to {}",
                self.site, self.slots.start, self.slots.end,
                self.slots.max_slot_size, reason
            )
        } else {
            format!(
                "Departure delay program at {} from {} to {} (flights depart at a rate of {} per hour)",
                self.site, self.slots.start, self.slots.end,
                self.slots.max_slot_size
            )
        }
    }

    fn arrival_airports_affected(&self) -> Vec<AirportCode> {
        Vec::new()
    }

    fn departure_airports_affected(&self) -> Vec<AirportCode> {
        vec![self.site]
    }
}

pub struct DisruptionIndex {
    disruptions: Vec<Arc<RwLock<dyn Disruption>>>,
    dep_index: HashMap<AirportCode, Vec<usize>>,
    arr_index: HashMap<AirportCode, Vec<usize>>,
}

impl DisruptionIndex {
    pub fn new() -> Self {
        Self {
            disruptions: Vec::new(),
            dep_index: HashMap::new(),
            arr_index: HashMap::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.disruptions.len()
    }

    pub fn add_disruption(&mut self, disruption: Arc<RwLock<dyn Disruption>>) {
        let index = self.disruptions.len();
        self.disruptions.push(disruption);
        {
            let reader = self.disruptions[index].read().unwrap();
            for dep_arpt in &reader.departure_airports_affected() {
                self.dep_index.entry(*dep_arpt).or_default().push(index);
            }
            for arr_arpt in &reader.arrival_airports_affected() {
                self.arr_index.entry(*arr_arpt).or_default().push(index);
            }
        }
    }

    pub fn lookup(&self, flight: &Flight) -> Vec<Arc<RwLock<dyn Disruption>>> {
        // Do not use the flight's departure time (look up result is reused for the disruption walk)
        let empty = Vec::new();
        let origin_disruptions = self.dep_index.get(&flight.origin).unwrap_or(&empty);
        let dest_disruptions = self.arr_index.get(&flight.dest).unwrap_or(&empty);
        origin_disruptions
            .iter()
            .chain(dest_disruptions.iter())
            .map(|index| self.disruptions[*index].clone())
            .collect()
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

    #[test]
    fn psg_demand_next() {
        let psg = PassengerDemand {
            count: 200,
            path: vec![
                AirportCode::from(&"DEN".to_owned()),
                AirportCode::from(&"MDW".to_owned()),
                AirportCode::from(&"BWI".to_owned()),
            ],
            flights_taken: Vec::new(),
        };
        assert_eq!(psg.next_dest(psg.path[0]), Some(psg.path[1]));
        assert_eq!(psg.next_dest(psg.path[1]), Some(psg.path[2]));
        assert_eq!(psg.next_dest(psg.path[2]), None);
    }

    #[test]
    fn slot_assign_immediate() {
        let now = Utc::now();
        let mut man = SlotManager::<FlightId>::new(now, now + TimeDelta::hours(1), 10);
        let ask_time = now + TimeDelta::minutes(10);
        let allocation = man.allocate_slot(&ask_time, 3314);
        assert_eq!(allocation, Some(now));
    }

    #[test]
    fn cssm_constant_rate_assign() {
        let now = Utc::now();
        let man = CumulativeSmallSlotManager::<FlightId>::new(now.clone(), vec![1, 1, 1, 1]);
        let allocation = man.allocate_slot(&now, 812);
        assert_eq!(allocation, Some(now));
    }

    /// Check the condition that ensures that the assigned accumulation never exceeds the allocated accumulation.
    #[test]
    fn cssm_constant_rate_exceeding() {
        let now = Utc::now();
        let man = CumulativeSmallSlotManager::<FlightId>::new(now.clone(), vec![1, 1, 1, 1]);
        
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(130)), 24).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(130)), 26).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(140)), 25).is_some());

        assert_eq!(man.allocate_slot(&(now + TimeDelta::minutes(140)), 80), Some(now + TimeDelta::hours(3)));
        assert_eq!(man.allocate_slot(&now, 99), None);
    }

    /// Check the condition that each hour's assigned slot count never exceeds the expected slot count + some margin
    #[test]
    fn cssm_slot_count_margin() {
        let now = Utc::now();
        let man = CumulativeSmallSlotManager::<FlightId>::new(now.clone(), vec![5, 5, 5, 1]);
        
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(200)), 24).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(210)), 67).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(220)), 80).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(225)), 31).is_some());

        assert_eq!(man.allocate_slot(&(now + TimeDelta::minutes(230)), 90), None);
    }
    
    #[test]
    fn cssm_limit_hit_many_times() {
        // throughput: 1 2 1 2 1
        // accumulation limit: 1 3 4 6 7
        // precondition: 0 3 0 3 0
        // precondition accumulation: 0 3(!) 3 6(!) 6
        let now = Utc::now();
        let man = CumulativeSmallSlotManager::<FlightId>::new(now.clone(), vec![1, 2, 1, 2, 1]);
        
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(80)), 24).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(100)), 67).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(110)), 80).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(190)), 31).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(200)), 12).is_some());
        assert!(man.allocate_slot(&(now + TimeDelta::minutes(210)), 26).is_some());
        
        // This should be delayed to the last hour because any earlier would make the accumulation surpass the limit.
        // (if we insert it in hour 0, then the accumulation would be 1 4 4 7 7, which violates 1 3 4 6 7.)
        let slot_result = man.allocate_slot(&(now + TimeDelta::minutes(10)), 90);
        assert!(slot_result.is_some());
        assert!(matches!(slot_result, Some(edct) if edct >= now + TimeDelta::hours(4) && edct <= now + TimeDelta::hours(5)));
    }

    #[test]
    fn cssm_slot_exists_already() {

    }
}
