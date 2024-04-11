//! Entity responsible for assigning resources to flights and executing them by realizing
//! changes to the model's data structures.
//!
//! Responsibilities:
//! - Try to execute all planned flights
//!   - Perform final assignment of resources (crew, aircraft) to flights
//!   - Delay flights if they lack resources
//!   - Request departure from all `Disruption`s, and follow all delays given

use std::{
    collections::BinaryHeap,
    sync::{Arc, RwLock},
};

use crate::{
    aircraft::{Flight, FlightId},
    model::Model,
};
use chrono::{DateTime, Utc};

pub enum UpdateType {
    CheckDepart,
    Depart,
    CheckArrive,
    Arrive,
}

pub struct DispatcherUpdate {
    pub time: DateTime<Utc>,
    pub flight: FlightId,
    pub _type: UpdateType,
}

impl PartialEq for DispatcherUpdate {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.flight as *const Flight == other.flight as *const Flight
    }
}
impl Eq for DispatcherUpdate {}

impl PartialOrd for DispatcherUpdate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.time.partial_cmp(&self.time)
    }
}
impl Ord for DispatcherUpdate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.time.cmp(&self.time)
    }
}

pub struct Dispatcher {
    model: Arc<RwLock<Model>>,
    /// If true, do not attempt to reassign resources to improve the network.
    /// If false, consider reassignments if the planned timing (departure, arrival times) cannot be achieved.
    follow_resource_plan: bool,

    update_queue: BinaryHeap<DispatcherUpdate>,
}

impl Dispatcher {
    /// Check the status of the given `flight`.
    /// If possible, move its progress forward.
    ///
    /// Flight stages:
    /// - Scheduled. Transition to Enroute is only possible when:
    ///   - The scheduled departure time has been reached
    ///   - Crew is available
    ///   - Aircraft is available
    ///   - Departure clearance is given by all Disruptions
    ///   - Airport's departure rate
    /// - Enroute. Transition to Arrived is only possible when:
    ///   - The scheduled time enroute has elapsed since departure
    ///   - Landing clearance is given by all Disruptions
    pub fn update_flight(&mut self, update: DispatcherUpdate) {
        match update._type {
            UpdateType::CheckDepart => {
                let model = self.model.read().unwrap();
                let flt = model.flight_read(update.flight);
                if flt.sched_depart > model.now {
                    self.update_queue.push(DispatcherUpdate {
                        flight: update.flight,
                        time: flt.sched_depart,
                        _type: update._type,
                    });
                }
                todo!()
            }
            UpdateType::Depart => {
                let mut model = self.model.write().unwrap();
                model.depart_flight(update.flight);
            }
            UpdateType::CheckArrive => {
                todo!()
            }
            UpdateType::Arrive => {
                let mut model = self.model.write().unwrap();
                model.arrive_flight(update.flight);
            }
        }
    }
}
