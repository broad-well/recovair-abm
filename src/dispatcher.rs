//! Entity responsible for assigning resources to flights and executing them by realizing
//! changes to the model's data structures.
//!
//! Responsibilities:
//! - Try to execute all planned flights
//!   - Perform final assignment of resources (crew, aircraft) to flights
//!   - Delay flights if they lack resources
//!   - Request departure from all `Disruption`s, and follow all delays given

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    ops::DerefMut,
    sync::Arc,
};

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
    fn reassign_suggestions(&self, _model: &Model) -> HashMap<FlightId, String> {
        HashMap::new()
    }

    fn on_flight_cancel(&mut self, _flight: FlightId, _model: &Model) {}
    fn on_flight_depart(&mut self, _flight: FlightId, _model: &Model) {}
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

    fn on_flight_cancel(&mut self, _flight: FlightId, _model: &Model) {}
    fn on_flight_depart(&mut self, _flight: FlightId, _model: &Model) {}
}

pub struct Dispatcher {
    pub model: Arc<Model>,
    pub aircraft_selector: Option<Box<dyn AircraftSelectionStrategy>>,
    pub crew_selector: Option<Box<dyn CrewSelectionStrategy>>,
    pub wait_for_deadheaders: bool,
    pub use_fallback_aircraft_selector: bool,

    pub aircraft_tolerance_before_reassign: TimeDelta,
    pub crew_tolerance_before_reassign: TimeDelta,

    pub update_queue: BinaryHeap<DispatcherUpdate>,
    pub aircraft_reassigned: HashSet<FlightId>,
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
            // if update.time > self.model.end {
            //     break;
            // }
            {
                *self.model._now.write().unwrap() = update.time;
            }
            self.update_flight(update);
        }

        // for update in &self.update_queue {
        //     self.model.cancel_flight(update.flight, CancelReason::DelayTimedOut);
        // }
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
                // Is there an assigned aircraft, and is the assigned aircraft available?
                {
                    let flt = self.model.flight_read(update.flight);
                    let ac_avail = if let Some(tail) = &flt.aircraft_tail {
                        let aircraft = self.model.fleet.get(tail).unwrap().read().unwrap();
                        aircraft.available_time(&*self.model, &flt)
                    } else {
                        None
                    };
                    if ac_avail
                        .map(|d| d > self.model.now() + self.aircraft_tolerance_before_reassign)
                        .unwrap_or(true)
                    {
                        // Unavailable
                        if let Some(ref mut selector) = &mut self.aircraft_selector {
                            let original_acft = flt.aircraft_tail.clone();
                            // There is a reassigner
                            // However, if there is a reassigner and the aircraft assignment is from the reassigner, trust it for now
                            if flt.aircraft_tail.is_none()
                                || !self.aircraft_reassigned.contains(&flt.id)
                            {
                                send_event!(
                                    self.model,
                                    ModelEventType::AircraftSelection(
                                        flt.id,
                                        flt.aircraft_tail.clone()
                                    )
                                );
                                drop(flt);
                                let ac = selector.deref_mut().select(update.flight, &self.model);
                                // Apply suggestions
                                for (flight, aircraft) in
                                    selector.deref_mut().reassign_suggestions(&self.model)
                                {
                                    let mut flt = self.model.flight_write(flight);
                                    if flt.took_off() {
                                        continue;
                                    }

                                    let reassigned = flt.reassign_aircraft(aircraft.clone());
                                    self.aircraft_reassigned.insert(flight);
                                    if reassigned {
                                        send_event!(
                                            self.model,
                                            ModelEventType::AircraftAssignmentChanged(
                                                flight, aircraft
                                            )
                                        );
                                    }
                                }
                                if let Some(ac) = ac {
                                    // Reassignment is given
                                    self.aircraft_reassigned.insert(update.flight);
                                    let flt = self.model.flight_read(update.flight);
                                    assert_eq!(flt.aircraft_tail, Some(ac.clone()));
                                    self.model.fleet[&ac].write().unwrap().claim(flt.id);
                                    send_event!(
                                        self.model,
                                        ModelEventType::AircraftAssignmentChanged(
                                            flt.id,
                                            ac.clone()
                                        )
                                    );
                                    self.update_queue.push(DispatcherUpdate {
                                        flight: update.flight,
                                        time: self.model.now(),
                                        _type: UpdateType::CheckDepart,
                                    });
                                    drop(flt);
                                } else {
                                    // Keep waiting. Maybe it can have a reassignment later
                                    // TODO make it possible to configure whether to cancel here
                                    self.delay_departure(
                                        self.model.now(),
                                        update.flight,
                                        vec![(
                                            RESOURCE_WAIT,
                                            DelayReason::AircraftShortage(original_acft),
                                        )],
                                    );
                                }
                                return;
                            } else {
                                drop(flt);
                                self.delay_departure(
                                    self.model.now(),
                                    update.flight,
                                    vec![(
                                        RESOURCE_WAIT,
                                        DelayReason::AircraftShortage(original_acft),
                                    )],
                                );
                                return;
                            }
                        } else {
                            // Can't deviate, must wait
                            // Use the fallback selector: Pick the aircraft that will be able to serve this flight the earliest
                            drop(flt); // Switch to a read so that Aircraft::available_time doesn't cause a deadlock
                            let flt = self.model.flight_read(update.flight);
                            send_event!(
                                self.model,
                                ModelEventType::AircraftSelection(
                                    update.flight,
                                    flt.aircraft_tail.clone()
                                )
                            );
                            // TODO consider incoming flights
                            let aircraft_cands: Vec<(String, DateTime<Utc>)> =
                                if self.use_fallback_aircraft_selector {
                                    self.model
                                        .airports
                                        .get(&flt.origin)
                                        .unwrap()
                                        .read()
                                        .unwrap()
                                        .fleet
                                        .iter()
                                        .filter_map(|aircraft_id| {
                                            let avail = self
                                                .model
                                                .fleet
                                                .get(aircraft_id)
                                                .unwrap()
                                                .read()
                                                .unwrap()
                                                .available_time(&self.model, &flt);
                                            avail.map(|i| (aircraft_id.clone(), i))
                                        })
                                        .collect()
                                } else {
                                    Vec::new()
                                };
                            drop(flt);
                            let (new_acft, delay_duration): (Option<String>, Option<TimeDelta>) =
                                if aircraft_cands.is_empty() {
                                    (None, Some(RESOURCE_WAIT))
                                } else {
                                    let selected_aircraft =
                                        aircraft_cands.into_iter().min_by_key(|i| i.1).unwrap();
                                    let mut flt = self.model.flight_write(update.flight);
                                    flt.reassign_aircraft(selected_aircraft.0.clone());
                                    {
                                        self.model.fleet[&selected_aircraft.0]
                                            .write()
                                            .unwrap()
                                            .claim(flt.id);
                                    }
                                    send_event!(
                                        self.model,
                                        ModelEventType::AircraftAssignmentChanged(
                                            flt.id,
                                            selected_aircraft.0.clone()
                                        )
                                    );
                                    (
                                        Some(selected_aircraft.0),
                                        if selected_aircraft.1 <= self.model.now() {
                                            None
                                        } else {
                                            Some(selected_aircraft.1 - self.model.now())
                                        },
                                    )
                                };

                            if let Some(delay_duration) = delay_duration {
                                self.delay_departure(
                                    self.model.now(),
                                    update.flight,
                                    vec![(delay_duration, DelayReason::AircraftShortage(new_acft))],
                                );
                                return;
                            }
                        }
                    } else if ac_avail.unwrap() > self.model.now() {
                        // Delay within tolerance
                        let tail = flt.aircraft_tail.clone();
                        // Make sure that we claim it!
                        self.model.fleet[tail.as_ref().unwrap()]
                            .write()
                            .unwrap()
                            .claim(update.flight);

                        drop(flt);
                        self.delay_departure(
                            self.model.now(),
                            update.flight,
                            vec![(
                                ac_avail.unwrap() - self.model.now(),
                                DelayReason::AircraftShortage(tail),
                            )],
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
                    } else if unavailable_crew.is_empty() {
                        &[]
                    } else {
                        &unavailable_crew[0..1]
                    };

                    let needs_reassignment = critical_crew_set
                        .iter()
                        .filter(|(_, time)| requires_reassignment(&time))
                        .map(|i| i.0)
                        .collect::<Vec<_>>();
                    if !needs_reassignment.is_empty() || flt.crew.is_empty() {
                        if let Some(ref mut selector) = &mut self.crew_selector {
                            send_event!(
                                self.model,
                                ModelEventType::CrewSelection(flt.id, needs_reassignment.clone())
                            );
                            if let Some(crews) =
                                selector.select(flt.id, &self.model, needs_reassignment.clone())
                            {
                                // Reassignment made
                                flt.reassign_crew(crews.clone());
                                // We assume that all crews assigned at this point will be available soon (within crew_tolerance_before_reassign)
                                // We also require all of them to either be on the flight to the origin or already at the origin.
                                for crew in &crews {
                                    self.model.crew[crew].write().unwrap().claim(flt.id);
                                }
                                send_event!(
                                    self.model,
                                    ModelEventType::CrewAssignmentChanged(flt.id, crews)
                                );
                                self.update_queue.push(DispatcherUpdate {
                                    flight: update.flight,
                                    time: self.model.now(),
                                    _type: UpdateType::CheckDepart,
                                });
                                return;
                            } else {
                                // No reassignment, must cancel
                                drop(flt);
                                self.cancel_flight(
                                    update.flight,
                                    CancelReason::HeavyExpectedDelay(DelayReason::CrewShortage(
                                        needs_reassignment,
                                    )),
                                );
                                return;
                            }
                        } else {
                            // No crew selector, just wait
                            let mut delay_decision = RESOURCE_WAIT;
                            let mut delay_cause: Option<Vec<CrewId>> = None;
                            if flt.crew.is_empty()
                                && !self.model.airports[&flt.origin]
                                    .read()
                                    .unwrap()
                                    .crew
                                    .is_empty()
                            {
                                // Fallback selector: Pick the crew that can take this flight most immediately
                                let arpt = self.model.airports[&flt.origin].read().unwrap();
                                let best_crew = arpt
                                    .crew
                                    .iter()
                                    .map(|id| {
                                        let crew = self.model.crew[id].read().unwrap();
                                        (
                                            id,
                                            crew.time_until_available_for(
                                                &flt,
                                                self.model.now(),
                                                &self.model,
                                            ),
                                        )
                                    })
                                    .filter(|i| i.1.is_some())
                                    .map(|i| (i.0, i.1.unwrap()))
                                    .min_by_key(|i| i.1);
                                if let Some((best_id, wait_time)) = best_crew {
                                    flt.reassign_crew(vec![*best_id]);
                                    self.model.crew[best_id].write().unwrap().claim(flt.id);
                                    send_event!(
                                        self.model,
                                        ModelEventType::CrewAssignmentChanged(
                                            flt.id,
                                            vec![*best_id]
                                        )
                                    );
                                    if wait_time <= TimeDelta::zero() {
                                        self.update_queue.push(DispatcherUpdate {
                                            flight: update.flight,
                                            time: self.model.now(),
                                            _type: UpdateType::CheckDepart,
                                        });
                                        return;
                                    }
                                    delay_decision = wait_time;
                                    delay_cause = Some(vec![*best_id]);
                                }
                            }
                            drop(flt);
                            self.delay_departure(
                                self.model.now(),
                                update.flight,
                                vec![(
                                    delay_decision,
                                    DelayReason::CrewShortage(
                                        delay_cause.unwrap_or(needs_reassignment),
                                    ),
                                )],
                            );
                            return;
                        }
                    } else {
                        let (max_wait_cause, max_wait) = critical_crew_set
                            .iter()
                            .max_by_key(|i| i.1.unwrap())
                            .unwrap();
                        let max_wait = max_wait.unwrap();
                        if max_wait > TimeDelta::zero() {
                            drop(flt);
                            self.delay_departure(
                                self.model.now(),
                                update.flight,
                                vec![(max_wait, DelayReason::CrewShortage(vec![*max_wait_cause]))],
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
                    debug_assert!(flt.crew[0] == prev_pilot);
                }
                // Are disruptions preventing this flight from taking off?
                {
                    if let Some((clear, disruption)) = self.model.request_departure(update.flight) {
                        if let Some(later) = clear.time() {
                            if later > &self.model.now() {
                                // Disruption delayed the flight
                                let delay_dist = disruption
                                    .into_iter()
                                    .map(|(disruption, _, amount)| {
                                        (
                                            amount,
                                            DelayReason::Disrupted(
                                                disruption.read().unwrap().describe(),
                                            ),
                                        )
                                    })
                                    .collect::<Vec<_>>();

                                self.delay_departure(self.model.now(), update.flight, delay_dist);
                                return;
                            }
                        }
                    } else {
                        // Request determined that accumulated delay would exceed max_delay
                        self.cancel_flight(update.flight, CancelReason::DelayTimedOut);
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
                        let delay = airport_depart_time - self.model.now();
                        drop(arpt);
                        self.delay_departure(
                            self.model.now(),
                            update.flight,
                            vec![(delay, DelayReason::RateLimited(origin))],
                        );
                        return;
                    }
                }
                // Everything is available
                self.depart_flight(update.flight);
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
                    if let Some((clearance, disruption)) = self.model.request_arrival(update.flight)
                    {
                        if let Some(time) = clearance.time() {
                            let delay_dist = disruption
                                .into_iter()
                                .map(|(disruption, _, amount)| {
                                    (
                                        amount,
                                        DelayReason::Disrupted(
                                            disruption.read().unwrap().describe(),
                                        ),
                                    )
                                })
                                .collect::<Vec<_>>();
                            for (delay, reason) in delay_dist {
                                send_event!(
                                    self.model,
                                    ModelEventType::FlightArrivalDelayed(
                                        update.flight,
                                        delay,
                                        reason
                                    )
                                );
                            }
                            {
                                let mut flt = self.model.flight_write(update.flight);
                                flt.delay_arrival(*time - self.model.now());
                            }
                            self.update_queue.push(DispatcherUpdate {
                                flight: update.flight,
                                time: *time,
                                _type: UpdateType::CheckArrive,
                            });
                            return;
                        }
                    } else {
                        panic!("No handler for arrival delay past max_delay")
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
                        {
                            let mut flt = self.model.flight_write(update.flight);
                            flt.delay_arrival(arrive_time - self.model.now());
                        }
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
        &mut self,
        now: DateTime<Utc>,
        id: FlightId,
        reason: Vec<(TimeDelta, DelayReason)>,
    ) {
        let duration: TimeDelta = reason.iter().map(|i| i.0).sum();
        debug_assert!(duration > TimeDelta::zero());
        {
            let sched_depart = self.model.flight_read(id).sched_depart;
            if now + duration > sched_depart + self.model.config.max_delay {
                // Exceeding max delay, need to cancel
                self.cancel_flight(id, CancelReason::DelayTimedOut);
                return;
            }
        }
        for comp in reason {
            send_event!(
                self.model,
                ModelEventType::FlightDepartureDelayed(id, comp.0, comp.1)
            );
        }
        self.model.flight_write(id).delay_departure(duration);
        self.update_queue.push(DispatcherUpdate {
            flight: id,
            time: now + duration,
            _type: UpdateType::CheckDepart,
        });
    }

    fn cancel_flight(&mut self, flight: FlightId, reason: CancelReason) {
        self.model.cancel_flight(flight, reason);
        if let Some(ref mut selector) = self.aircraft_selector {
            selector.on_flight_cancel(flight, &self.model);
        }
        if let Some(ref mut selector) = self.crew_selector {
            selector.on_flight_cancel(flight, &self.model);
        }
    }

    fn depart_flight(&mut self, id: FlightId) {
        self.model.depart_flight(id);
        if let Some(ref mut selector) = self.aircraft_selector {
            selector.on_flight_depart(id, &self.model);
        }
        if let Some(ref mut selector) = self.crew_selector {
            selector.on_flight_depart(id, &self.model);
        }
    }
}

// MARK: Strategies

pub mod strategies {
    use super::*;
    use crate::airport::AirportCode;
    use std::collections::HashMap;

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

    struct DfsAircraftSelectionStrategy {
        surplus_aircraft: Vec<(DateTime<Utc>, String, AirportCode)>,
        unfulfilled: HashMap<AirportCode, Vec<FlightId>>,
        cached_reservations: Option<HashMap<FlightId, String>>,
        last_ran: Option<DateTime<Utc>>,
    }

    impl DfsAircraftSelectionStrategy {
        pub fn new() -> Self {
            Self {
                surplus_aircraft: Vec::new(),
                unfulfilled: HashMap::new(),
                cached_reservations: None,
                last_ran: None,
            }
        }

        fn run_dfs(&self, model: &Model) -> HashMap<FlightId, String> {
            println!(
                "DFS debug: There are {} surplus aircraft and {} unfulfilled flights",
                self.surplus_aircraft.len(),
                self.unfulfilled.iter().map(|(_, v)| v.len()).sum::<usize>()
            );
            #[derive(Clone, Debug)]
            struct Node {
                trail: Vec<FlightId>,
                location: AirportCode,
                next_available: DateTime<Utc>,
                accum_delay: TimeDelta,
            }

            let mut reservations: HashMap<FlightId, String> = HashMap::new();
            let mut num_aircraft_with_path = 0u32;
            for (start_time, aircraft, origin) in &self.surplus_aircraft {
                let mut frontier: Vec<Node> = vec![Node {
                    trail: Vec::new(),
                    location: *origin,
                    next_available: *start_time,
                    accum_delay: TimeDelta::zero(),
                }];
                let mut longest: Option<Node> = None;
                while let Some(node) = frontier.pop() {
                    // println!("searching for {}: {:?}", aircraft, &node);
                    if longest.is_none()
                        || longest.as_ref().unwrap().trail.len() < node.trail.len()
                        || (longest.as_ref().unwrap().trail.len() == node.trail.len()
                            && longest.as_ref().unwrap().accum_delay > node.accum_delay)
                    {
                        longest = Some(node.clone());
                    }
                    let Node {
                        trail,
                        location,
                        next_available,
                        accum_delay,
                    } = node;
                    if trail.len() > 4 {
                        continue;
                    }

                    let next: Vec<u64> = if let Some(flights) = self.unfulfilled.get(&location) {
                        Box::new(
                            flights
                                .into_iter()
                                .filter(|next_flight| {
                                    if reservations.contains_key(next_flight) {
                                        false
                                    } else if let Ok(flight) =
                                        model.flights[*next_flight].try_read()
                                    {
                                        flight.sched_depart + flight.dep_delay - next_available
                                            > TimeDelta::hours(-2)
                                            && next_available - flight.sched_depart
                                                < model.config.max_delay
                                    } else {
                                        false
                                    }
                                })
                                .map(|i| *i),
                        )
                        .collect()
                    } else {
                        Vec::new()
                    };
                    for next_flight in next {
                        let flight_info = model.flight_read(next_flight);
                        let depart_time = std::cmp::max(
                            flight_info.sched_depart + flight_info.dep_delay,
                            next_available,
                        );
                        // Note that accum_delay here means delays incurred by aircraft shortage
                        let delay =
                            depart_time - (flight_info.sched_depart + flight_info.dep_delay);
                        let mut next_trail = trail.clone();
                        next_trail.push(next_flight);
                        let time_available_after = depart_time
                            + flight_info.est_duration()
                            + model.config.aircraft_turnaround_time;
                        frontier.push(Node {
                            trail: next_trail,
                            location: flight_info.dest,
                            next_available: time_available_after,
                            accum_delay: accum_delay + delay,
                        });
                    }
                }
                if let Some(longest) = longest {
                    println!(
                        "DFS resolved: Path for {} (currently at {}) should be {:?}",
                        aircraft, origin, longest
                    );
                    if !longest.trail.is_empty() {
                        num_aircraft_with_path += 1;
                    }
                    for flight in longest.trail {
                        reservations.insert(flight, aircraft.clone());
                    }
                }
            }
            
            println!(
                "[[DFS STATS]] [{}, {}, {}, {}, {}]",
                model.now(),
                self.unfulfilled.values().map(Vec::len).sum::<usize>(),
                reservations.len(),
                self.surplus_aircraft.len(),
                num_aircraft_with_path
            );
            reservations
        }

        fn remove_stale_flights(&mut self, model: &Model) {
            for (_, v) in self.unfulfilled.iter_mut() {
                v.retain(|f| {
                    let flt = model.flight_read(*f);
                    flt.sched_depart > model.now() - TimeDelta::hours(4) && !flt.cancelled
                });
            }
        }
    }

    impl AircraftSelectionStrategy for DfsAircraftSelectionStrategy {
        fn select(&mut self, _flight: FlightId, _model: &Model) -> Option<String> {
            if self.last_ran.is_none()
                || self.last_ran.unwrap() < _model.now() - TimeDelta::minutes(15)
            {
                self.remove_stale_flights(_model);
                let reservations = self.run_dfs(_model);
                self.cached_reservations = Some(reservations);
                self.last_ran = Some(_model.now());
            }
            // println!("DFS output: {:?}", self.cached_reservations);
            self.cached_reservations
                .as_ref()
                .unwrap()
                .get(&_flight)
                .cloned()
        }

        fn reassign_suggestions(&self, _model: &Model) -> HashMap<FlightId, String> {
            self.cached_reservations
                .as_ref()
                .unwrap_or(&HashMap::new())
                .clone()
        }

        fn on_flight_cancel(&mut self, flight: FlightId, _model: &Model) {
            // Available assigned aircraft --> surplus aircraft
            let flight = _model.flight_read(flight);
            if let Some(tail) = &flight.aircraft_tail {
                let acft = _model.fleet[tail].read().unwrap();
                if let Some(avail_time) = acft.available_time(_model, &flight) {
                    self.surplus_aircraft.retain(|i| i.1 != *tail);
                    let index = match self
                        .surplus_aircraft
                        .binary_search_by_key(&avail_time, |i| i.0)
                    {
                        Ok(n) => n,
                        Err(n) => n,
                    };
                    self.surplus_aircraft
                        .insert(index, (avail_time, tail.clone(), flight.origin));
                }
                // Downstream flights --> unfulfilled
                for (id, v) in &_model.flights {
                    if let Ok(next) = v.read() {
                        if next.aircraft_tail.as_ref() == Some(tail)
                            && next.sched_depart > flight.sched_depart
                            && !next.cancelled
                        {
                            self.unfulfilled
                                .entry(next.origin)
                                .or_insert_with(Vec::new)
                                .push(*id);
                        }
                    }
                }
            }
        }

        fn on_flight_depart(&mut self, _flight: FlightId, _model: &Model) {
            let flight = _model.flight_read(_flight);
            let acft = flight
                .aircraft_tail
                .as_ref()
                .expect("Departed flight must have assigned aircraft!");
            if let Some(index) = self
                .surplus_aircraft
                .iter()
                .position(|(_, v, _)| *v == *acft)
            {
                self.surplus_aircraft.remove(index);
            }
            self.unfulfilled.entry(flight.origin).and_modify(|vec| {
                if let Some(i) = vec.iter().position(|id| *id == _flight) {
                    vec.remove(i);
                }
            });
        }
    }

    pub fn new_for_aircraft(key: &str) -> Box<dyn AircraftSelectionStrategy> {
        match key {
            "giveup" => Box::new(GiveUpAircraftSelectionStrategy {}),
            "dfs" => Box::new(DfsAircraftSelectionStrategy::new()),
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
