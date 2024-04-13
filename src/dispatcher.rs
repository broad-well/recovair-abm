//! Entity responsible for assigning resources to flights and executing them by realizing
//! changes to the model's data structures.
//!
//! Responsibilities:
//! - Try to execute all planned flights
//!   - Perform final assignment of resources (crew, aircraft) to flights
//!   - Delay flights if they lack resources
//!   - Request departure from all `Disruption`s, and follow all delays given

use std::{collections::BinaryHeap, ops::DerefMut, sync::Arc};

use crate::{
    aircraft::{Flight, FlightId},
    crew::CrewId,
    metrics::{CancelReason, DelayReason, ModelEvent, ModelEventType},
    model::Model,
};
use chrono::{DateTime, TimeDelta, Utc};

pub enum UpdateType {
    CheckDepart,
    CheckArrive,
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
    /// Reassign the given flight to any aircraft. The aircraft should be at the flight's origin
    /// or arriving at the flight's origin.
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
    pub model: Arc<Model>,
    pub aircraft_selector: Option<Box<dyn AircraftSelectionStrategy>>,
    pub crew_selector: Option<Box<dyn CrewSelectionStrategy>>,
    pub wait_for_deadheaders: bool,

    pub aircraft_tolerance_before_reassign: TimeDelta,
    pub crew_tolerance_before_reassign: TimeDelta,

    pub update_queue: BinaryHeap<DispatcherUpdate>,
}

macro_rules! send_event {
    ( $self:expr, $ev:expr ) => {{
        $self
            .publisher
            .send(ModelEvent {
                time: $self.now(),
                data: $ev,
            })
            .expect("Metrics collector dropped");
    }};
}

impl Dispatcher {
    /// Enqueue all upcoming flights for `CheckDepart`.
    pub fn init_flight_updates(&mut self) {
        for flight in self.model.flights.values() {
            let flight = flight.read().unwrap();
            self.update_queue.push(DispatcherUpdate {
                time: flight.sched_depart,
                flight: flight.id,
                _type: UpdateType::CheckDepart,
            });
        }
    }

    /// Run the entire network model by successively processing updates
    /// and sending out ModelEvents.
    ///
    /// Note: Since `run_model` updates `model._now`, it must borrow the model
    /// mutably.
    pub fn run_model(&mut self) {
        send_event!(
            self.model,
            ModelEventType::SimulationStarted(Arc::downgrade(&self.model))
        );

        while let Some(update) = self.update_queue.pop() {
            assert!(self.model.now() <= update.time);
            {
                *self.model._now.write().unwrap() = update.time;
            }
            assert!(self.model.now() == update.time);
            self.update_flight(update);
        }

        send_event!(self.model, ModelEventType::SimulationComplete);
    }
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
        match update._type {
            UpdateType::CheckDepart => {
                {
                    let flt = self.model.flight_write(update.flight);
                    // Has the scheduled departure time been reached?
                    if flt.sched_depart > self.model.now() {
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
                    let mut flt = self.model.flight_write(update.flight);
                    let aircraft = self
                        .model
                        .fleet
                        .get(&flt.aircraft_tail)
                        .unwrap()
                        .read()
                        .unwrap();
                    let ac_avail = aircraft.available_time(&*self.model, flt.origin);
                    if ac_avail
                        .map(|d| d > self.model.now() + self.aircraft_tolerance_before_reassign)
                        .unwrap_or(true)
                    {
                        // Unavailable
                        if let Some(ref mut selector) = &mut self.aircraft_selector {
                            // There is a reassigner
                            send_event!(
                                self.model,
                                ModelEventType::AircraftSelection(
                                    flt.id,
                                    flt.aircraft_tail.clone()
                                )
                            );
                            if let Some(ac) = selector.deref_mut().select(flt.id, &self.model) {
                                // Reassignment is given
                                flt.reassign_aircraft(ac.clone());
                                send_event!(
                                    self.model,
                                    ModelEventType::AircraftAssignmentChanged(flt.id)
                                );
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: self.model.now(),
                                    _type: UpdateType::CheckDepart,
                                });
                                return;
                            } else {
                                // Can only cancel
                                self.model.cancel_flight(
                                    update.flight,
                                    CancelReason::HeavyExpectedDelay(DelayReason::AircraftShortage),
                                );
                                return;
                            }
                        } else {
                            // Can't deviate, must wait
                            Self::delay_departure(
                                self.model.now(),
                                &self.model,
                                update.flight,
                                RESOURCE_WAIT,
                                DelayReason::AircraftShortage,
                                &mut self.update_queue,
                            );
                            return;
                        }
                    } else if ac_avail.unwrap() > self.model.now() {
                        // Delay within tolerance
                        Self::delay_departure(
                            self.model.now(),
                            &self.model,
                            update.flight,
                            ac_avail.unwrap() - self.model.now(),
                            DelayReason::AircraftShortage,
                            &mut self.update_queue,
                        );
                        return;
                    }
                }
                // TODO reduce duplication somehow, maybe by unifying Crew and Aircraft under a trait "Resource"
                // Is all the assigned crew available?
                {
                    let mut flt = self.model.flight_write(update.flight);
                    let unavailable_crew = flt
                        .crew
                        .iter()
                        .map(|id| {
                            (
                                *id,
                                self.model
                                    .crew
                                    .get(id)
                                    .unwrap()
                                    .read()
                                    .unwrap()
                                    .time_until_available_for(&flt, self.model.now(), &self.model),
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
                                self.model,
                                ModelEventType::CrewSelection(flt.id, needs_reassignment.clone())
                            );
                            if let Some(crews) =
                                selector.select(flt.id, &self.model, needs_reassignment)
                            {
                                // Reassignment made
                                flt.reassign_crew(crews);
                                send_event!(
                                    self.model,
                                    ModelEventType::CrewAssignmentChanged(flt.id)
                                );
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: self.model.now(),
                                    _type: UpdateType::CheckDepart,
                                });
                                return;
                            } else {
                                // No reassignment, must cancel
                                self.model.cancel_flight(
                                    update.flight,
                                    CancelReason::HeavyExpectedDelay(DelayReason::CrewShortage),
                                );
                                return;
                            }
                        } else {
                            // No crew selector, just wait
                            Self::delay_departure(
                                self.model.now(),
                                &self.model,
                                update.flight,
                                RESOURCE_WAIT,
                                DelayReason::CrewShortage,
                                &mut self.update_queue,
                            );
                            return;
                        }
                    } else {
                        let max_wait = critical_crew_set
                            .iter()
                            .map(|i| i.1.unwrap())
                            .max()
                            .unwrap();
                        if max_wait > TimeDelta::zero() {
                            Self::delay_departure(
                                self.model.now(),
                                &self.model,
                                update.flight,
                                max_wait,
                                DelayReason::CrewShortage,
                                &mut self.update_queue,
                            );
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
                    let (clear, disruption) = self.model.request_departure(update.flight);
                    if let Some(later) = clear.time() {
                        // Disruption delayed the flight
                        Self::delay_departure(
                            self.model.now(),
                            &self.model,
                            update.flight,
                            *later - self.model.now(),
                            DelayReason::Disrupted(disruption.unwrap().read().unwrap().describe()),
                            &mut self.update_queue,
                        );
                        return;
                    }
                }
                let origin = {
                    let origin = self.model.flight_read(update.flight).origin;
                    origin
                };
                // When can the origin airport handle this departure?
                {
                    let arpt = self.model.airports.get(&origin).unwrap().read().unwrap();
                    let airport_depart_time = arpt.depart_time(self.model.now());
                    if airport_depart_time > self.model.now() {
                        Self::delay_departure(
                            self.model.now(),
                            &self.model,
                            update.flight,
                            airport_depart_time - self.model.now(),
                            DelayReason::RateLimited(origin),
                            &mut self.update_queue,
                        );
                        return;
                    }
                }
                // Everything is available
                self.model.depart_flight(update.flight);
                let arrive_time = self.model.flight_read(update.flight).act_arrive_time();
                self.update_queue.push(DispatcherUpdate {
                    flight: update.flight,
                    time: arrive_time,
                    _type: UpdateType::CheckArrive,
                });
            }
            UpdateType::CheckArrive => {
                // Has the flight time elapsed since departure?
                {
                    let flt = self.model.flight_read(update.flight);
                    let arrive_time = flt.act_arrive_time();
                    if arrive_time > self.model.now() {
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
                    let (clearance, disruption) = self.model.request_arrival(update.flight);
                    if let Some(time) = clearance.time() {
                        send_event!(
                            self.model,
                            ModelEventType::FlightArrivalDelayed(
                                update.flight,
                                *time - self.model.now(),
                                DelayReason::Disrupted(
                                    disruption.unwrap().read().unwrap().describe()
                                )
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
                    let dest = self.model.flight_read(update.flight).dest;
                    let dest_airport = self.model.airports.get(&dest).unwrap().read().unwrap();
                    let arrive_time = dest_airport.arrive_time(self.model.now());
                    if arrive_time > self.model.now() {
                        send_event!(
                            self.model,
                            ModelEventType::FlightArrivalDelayed(
                                update.flight,
                                arrive_time - self.model.now(),
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
                self.model.arrive_flight(update.flight);
            }
        }
    }

    fn delay_departure(
        now: DateTime<Utc>,
        model: &Model,
        id: FlightId,
        duration: TimeDelta,
        reason: DelayReason,
        queue: &mut BinaryHeap<DispatcherUpdate>,
    ) {
        {
            let sched_depart = model.flight_read(id).sched_depart;
            if now + duration > sched_depart + model.config.max_delay {
                // Exceeding max delay, need to cancel
                model.cancel_flight(id, CancelReason::DelayTimedOut);
                return;
            }
        }
        send_event!(
            model,
            ModelEventType::FlightDepartureDelayed(id, duration, reason)
        );
        queue.push(DispatcherUpdate {
            flight: id,
            time: now + duration,
            _type: UpdateType::CheckDepart,
        });
    }
}

// MARK: Strategies

pub mod strategies {
    use super::*;

    struct GiveUpAircraftSelectionStrategy {}
    impl AircraftSelectionStrategy for GiveUpAircraftSelectionStrategy {
        fn select(&mut self, _flight: FlightId, _model: &Model) -> Option<String> {
            None
        }
    }

    struct GiveUpCrewSelectionStrategy {}
    impl CrewSelectionStrategy for GiveUpCrewSelectionStrategy {
        fn select(
            &mut self,
            _flight: FlightId,
            _model: &Model,
            _unavailable_crew: Vec<CrewId>,
        ) -> Option<Vec<CrewId>> {
            None
        }
    }

    pub fn new_for_aircraft(key: &str) -> Box<dyn AircraftSelectionStrategy> {
        match key {
            "giveup" => Box::new(GiveUpAircraftSelectionStrategy {}),
            _ => unimplemented!("aircraft selection strategy {:?}", key),
        }
    }
    pub fn new_for_crew(key: &str) -> Box<dyn CrewSelectionStrategy> {
        match key {
            "giveup" => Box::new(GiveUpCrewSelectionStrategy {}),
            _ => unimplemented!("crew selection strategy {:?}", key),
        }
    }
}
