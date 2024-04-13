//! Entity responsible for assigning resources to flights and executing them by realizing
//! changes to the model's data structures.
//!
//! Responsibilities:
//! - Try to execute all planned flights
//!   - Perform final assignment of resources (crew, aircraft) to flights
//!   - Delay flights if they lack resources
//!   - Request departure from all `Disruption`s, and follow all delays given

use std::{
    collections::{BinaryHeap, HashMap},
    ops::DerefMut,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use crate::{
    aircraft::{Flight, FlightId},
    crew::CrewId,
    metrics::{DelayReason, ModelEvent, ModelEventType},
    model::Model,
};
use chrono::{DateTime, TimeDelta, Utc};

pub enum UpdateType {
    CheckDepart,
    CheckArrive,
    Cancel,
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

static RESOURCE_WAIT: TimeDelta = TimeDelta::minutes(10);

pub trait AircraftSelectionStrategy {
    /// Reassign the given flight to any aircraft. The aircraft should be expected to
    /// become available for the flight within a few hours of its scheduled departure time.
    ///
    /// To cancel the flight, return None.
    fn select(&mut self, flight: FlightId, model: &Model) -> Option<String>;
}

pub trait CrewSelectionStrategy {
    /// Reassign the given flight to any crew. The crew should be expected to
    /// become available for the flight within a few hours of its scheduled departure time.
    /// Compared to `AircraftSelectionStrategy`, this should also consider duty time.
    ///
    /// To cancel the flight, return None.
    fn select(
        &mut self,
        flight: FlightId,
        model: &Model,
        unavailable_crew: Vec<CrewId>,
    ) -> Option<Vec<CrewId>>;
}

pub struct Dispatcher {
    model: Arc<Model>,
    aircraft_selector: Option<Box<dyn AircraftSelectionStrategy>>,
    crew_selector: Option<Box<dyn CrewSelectionStrategy>>,
    wait_for_deadheaders: bool,

    aircraft_tolerance_before_reassign: TimeDelta,
    crew_tolerance_before_reassign: TimeDelta,

    update_queue: BinaryHeap<DispatcherUpdate>,
}

macro_rules! send_event {
    ( $self:expr, $ev:expr ) => {{
        $self
            .publisher
            .send(ModelEvent {
                time: $self.now,
                data: $ev,
            })
            .expect("Metrics collector dropped");
    }};
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
    ///   - Airport's departure rate permits the departure
    /// - Enroute. Transition to Arrived is only possible when:
    ///   - The scheduled time enroute has elapsed since departure
    ///   - Landing clearance is given by all Disruptions
    pub fn update_flight(&mut self, update: DispatcherUpdate) {
        let model = &self.model;
        match update._type {
            UpdateType::CheckDepart => {
                {
                    let flt = model.flight_write(update.flight);
                    // Has the scheduled departure time been reached?
                    if flt.sched_depart > model.now {
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: flt.sched_depart,
                            _type: UpdateType::CheckDepart,
                        });
                        return;
                    }
                }
                // Is the assigned aircraft available?
                {
                    let mut flt = model.flight_write(update.flight);
                    let aircraft = model.fleet.get(&flt.aircraft_tail).unwrap().read().unwrap();
                    let ac_avail = aircraft.available_time(&*model, flt.origin);
                    if ac_avail
                        .map(|d| d > model.now + self.aircraft_tolerance_before_reassign)
                        .unwrap_or(true)
                    {
                        // Unavailable
                        if let Some(ref mut selector) = &mut self.aircraft_selector {
                            // There is a reassigner
                            send_event!(
                                model,
                                ModelEventType::AircraftSelection(
                                    flt.id,
                                    flt.aircraft_tail.clone()
                                )
                            );
                            if let Some(ac) = selector.deref_mut().select(flt.id, &model) {
                                // Reassignment is given
                                flt.reassign_aircraft(ac.clone());
                                send_event!(
                                    model,
                                    ModelEventType::AircraftAssignmentChanged(flt.id)
                                );
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: model.now,
                                    _type: UpdateType::CheckDepart,
                                });
                                return;
                            } else {
                                // Can only cancel
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: model.now,
                                    _type: UpdateType::Cancel,
                                });
                                return;
                            }
                        } else {
                            // Can't deviate, must wait
                            send_event!(
                                model,
                                ModelEventType::FlightDepartureDelayed(
                                    update.flight,
                                    RESOURCE_WAIT,
                                    DelayReason::AircraftShortage
                                )
                            );
                            self.update_queue.push(DispatcherUpdate {
                                flight: update.flight,
                                time: model.now + RESOURCE_WAIT,
                                _type: UpdateType::CheckDepart,
                            });
                            return;
                        }
                    } else if ac_avail.unwrap() > model.now {
                        // Delay within tolerance
                        send_event!(
                            model,
                            ModelEventType::FlightDepartureDelayed(
                                update.flight,
                                ac_avail.unwrap() - model.now,
                                DelayReason::AircraftShortage
                            )
                        );
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: ac_avail.unwrap(),
                            _type: UpdateType::CheckDepart,
                        });
                        return;
                    }
                }
                // TODO reduce duplication somehow, maybe by unifying Crew and Aircraft under a trait "Resource"
                // Is all the assigned crew available?
                {
                    let mut flt = model.flight_write(update.flight);
                    let unavailable_crew = flt
                        .crew
                        .iter()
                        .map(|id| {
                            (
                                *id,
                                model
                                    .crew
                                    .get(id)
                                    .unwrap()
                                    .read()
                                    .unwrap()
                                    .time_until_available_for(&flt, model.now),
                            )
                        })
                        .collect::<Vec<_>>();
                    let requires_reassignment = |wait: &&Option<TimeDelta>| match *wait {
                        Some(time) => *time > self.crew_tolerance_before_reassign,
                        None => true,
                    };
                    let critical_crew_set = if self.wait_for_deadheaders {
                        &unavailable_crew[..]
                    } else {
                        &unavailable_crew[0..1]
                    };

                    let needs_reassignment = critical_crew_set
                        .iter()
                        .filter(|(_, time)| requires_reassignment(&time))
                        .map(|i| i.0)
                        .collect::<Vec<_>>();
                    if !needs_reassignment.is_empty() {
                        if let Some(ref mut selector) = &mut self.crew_selector {
                            send_event!(
                                model,
                                ModelEventType::CrewSelection(flt.id, needs_reassignment.clone())
                            );
                            if let Some(crews) = selector.select(flt.id, &model, needs_reassignment)
                            {
                                // Reassignment made
                                flt.reassign_crew(crews);
                                send_event!(model, ModelEventType::CrewAssignmentChanged(flt.id));
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: model.now,
                                    _type: UpdateType::CheckDepart,
                                });
                                return;
                            } else {
                                // No reassignment, must cancel
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: model.now,
                                    _type: UpdateType::Cancel,
                                });
                                return;
                            }
                        } else {
                            // No crew selector, just wait
                            send_event!(
                                model,
                                ModelEventType::FlightDepartureDelayed(
                                    update.flight,
                                    RESOURCE_WAIT,
                                    DelayReason::CrewShortage
                                )
                            );
                            self.update_queue.push(DispatcherUpdate {
                                flight: update.flight,
                                time: model.now + RESOURCE_WAIT,
                                _type: UpdateType::CheckDepart,
                            });
                            return;
                        }
                    } else {
                        let max_wait = critical_crew_set
                            .iter()
                            .map(|i| i.1.unwrap())
                            .max()
                            .unwrap();
                        if max_wait > TimeDelta::zero() {
                            send_event!(
                                model,
                                ModelEventType::FlightDepartureDelayed(
                                    update.flight,
                                    max_wait,
                                    DelayReason::CrewShortage
                                )
                            );
                            self.update_queue.push(DispatcherUpdate {
                                flight: update.flight,
                                time: model.now + max_wait,
                                _type: UpdateType::CheckDepart,
                            });
                            return;
                        }
                    }
                    // Remove unavailable deadheading crew
                    let prev_pilot = flt.crew[0];
                    flt.crew = unavailable_crew
                        .iter()
                        .filter(|(_, time)| time.unwrap() <= TimeDelta::zero())
                        .map(|i| i.0)
                        .collect();
                    assert!(flt.crew[0] == prev_pilot);
                }
                // Are disruptions preventing this flight from taking off?
                {
                    let (clear, disruption) = model.request_departure(update.flight);
                    if let Some(later) = clear.time() {
                        // Disruption delayed the flight
                        send_event!(
                            model,
                            ModelEventType::FlightDepartureDelayed(
                                update.flight,
                                *later - model.now,
                                DelayReason::Disrupted(Arc::downgrade(&disruption.unwrap()))
                            )
                        );
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: *later,
                            _type: UpdateType::CheckDepart,
                        });
                        return;
                    }
                }
                let origin = {
                    let origin = model.flight_read(update.flight).origin;
                    origin
                };
                // When can the origin airport handle this departure?
                {
                    let arpt = model.airports.get(&origin).unwrap().read().unwrap();
                    let airport_depart_time = arpt.depart_time(model.now);
                    if airport_depart_time > model.now {
                        send_event!(
                            model,
                            ModelEventType::FlightDepartureDelayed(
                                update.flight,
                                airport_depart_time - model.now,
                                DelayReason::RateLimited(origin)
                            )
                        );
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: airport_depart_time,
                            _type: UpdateType::CheckDepart,
                        });
                        return;
                    }
                }
                // Everything is available
                model.depart_flight(update.flight);
                let arrive_time = model.flight_read(update.flight).act_arrive_time();
                self.update_queue.push(DispatcherUpdate {
                    flight: update.flight,
                    time: arrive_time,
                    _type: UpdateType::CheckArrive
                });
            }
            UpdateType::CheckArrive => {
                // Has the flight time elapsed since departure?
                {
                    let flt = model.flight_read(update.flight);
                    let arrive_time = flt.act_arrive_time();
                    if arrive_time > model.now {
                        // Too soon!
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: arrive_time,
                            _type: UpdateType::CheckArrive,
                        });
                        return;
                    }
                }
                // Is the arrival blocked by a disruption?
                {
                    let (clearance, disruption) = model.request_arrival(update.flight);
                    if let Some(time) = clearance.time() {
                        send_event!(
                            model,
                            ModelEventType::FlightArrivalDelayed(
                                update.flight,
                                *time - model.now,
                                DelayReason::Disrupted(Arc::downgrade(&disruption.unwrap()))
                            )
                        );
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: *time,
                            _type: UpdateType::CheckArrive,
                        });
                        return;
                    }
                }
                // Can the destination airport accommodate the arrival?
                {
                    let dest = model.flight_read(update.flight).dest;
                    let dest_airport = model.airports.get(&dest).unwrap().read().unwrap();
                    let arrive_time = dest_airport.arrive_time(model.now);
                    if arrive_time > model.now {
                        send_event!(
                            model,
                            ModelEventType::FlightArrivalDelayed(
                                update.flight,
                                arrive_time - model.now,
                                DelayReason::RateLimited(dest)
                            )
                        );
                        self.update_queue.push(DispatcherUpdate {
                            flight: update.flight,
                            time: arrive_time,
                            _type: UpdateType::CheckArrive,
                        });
                        return;
                    }
                }
                model.arrive_flight(update.flight);
            }
            UpdateType::Cancel => {
                model.cancel_flight(update.flight);
            }
        }
    }
}
