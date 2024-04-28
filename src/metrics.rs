use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    sync::{mpsc, Weak},
    thread::{self, JoinHandle},
};

use chrono::{DateTime, Duration, TimeDelta, Utc};

use crate::{
    aircraft::FlightId,
    airport::AirportCode,
    crew::CrewId,
    model::Model,
};

pub struct ModelEvent {
    pub time: DateTime<Utc>,
    pub data: ModelEventType,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DelayReason {
    CrewShortage,
    AircraftShortage,
    Disrupted(String),
    RateLimited(AirportCode),
}

#[derive(Debug, Clone)]
pub enum CancelReason {
    HeavyExpectedDelay(DelayReason),
    DelayTimedOut,
}

#[derive(Debug, Clone)]
pub enum ModelEventType {
    SimulationStarted(Weak<Model>),
    // -- Flight lifecycle --
    // Sender: Dispatcher
    // (A Flight object does not change once arrived)
    FlightDepartureDelayed(FlightId, Duration, DelayReason),
    FlightCancelled(FlightId, CancelReason),
    FlightDeparted(FlightId),
    FlightArrivalDelayed(FlightId, Duration, DelayReason),
    FlightArrived(FlightId),

    // -- Aircraft stats --
    // Sender: Dispatcher
    AircraftTurnedAround(String, AirportCode, Duration),

    // -- Scheduled changes --
    // Sender: Dispatcher
    CrewAssignmentChanged(FlightId, Vec<CrewId>),
    AircraftAssignmentChanged(FlightId, String),

    // -- Decision points --
    // Sender: Dispatcher
    CrewSelection(FlightId, Vec<CrewId>),
    AircraftSelection(FlightId, Option<String>),

    // -- Completion --
    SimulationComplete,
}

pub struct MetricsProcessor {
    receiver: mpsc::Receiver<ModelEvent>,
    model: Weak<Model>,
    // more memory needed to compute KPIs
    /// On-time performance measurement. Delays are stored in minutes
    pub arrival_delays: Vec<u16>,
    /// (On-time flight count, total flight count, cancellation count)
    pub otp: BTreeMap<DateTime<Utc>, (u32, u32, u32)>,

    /// Delay cause distribution (departure)
    pub dep_delay_causes: HashMap<DelayReason, u32>,
    /// Delay cause distribution (arrival)
    pub arr_delay_causes: HashMap<DelayReason, u32>,
}

// impl MapElement for Airport {
//     fn encode(&self, cx: Rc<RefCell<FunctionContext>>) -> JsResult<'_, JsObject> {
//         let mut cx = cx.borrow_mut();
//         let obj = cx.empty_object();
//         object_set!(cx, obj, "code", cx.string(self.code.to_string()));
//         object_set!(cx, obj, "dep_rate", cx.number(self.max_dep_per_hour as f64));
//         object_set!(cx, obj, "arr_rate", cx.number(self.max_arr_per_hour as f64));
//         Ok(obj)
//     }
// }

// impl MapElement for Crew {
//     fn encode(&self, cx: Rc<RefCell<FunctionContext>>) -> JsResult<'_, JsObject> {
//         let mut cx = cx.borrow_mut();
//         let obj = cx.empty_object();
//         object_set!(cx, obj, "id", cx.number(self.id));
//         let path = cx.empty_array();
//         for (i, code) in self.duty.iter().enumerate() {
//             object_set!(cx, path, i as u32, cx.string(code.to_string()));
//         }
//         object_set!(cx, obj, "duty", path);
//         Ok(obj)
//     }
// }



impl MetricsProcessor {
    pub fn new(receiver: mpsc::Receiver<ModelEvent>) -> JoinHandle<MetricsProcessor> {
        let proc = Self {
            receiver,
            model: Weak::new(),
            arrival_delays: Vec::new(),
            dep_delay_causes: HashMap::new(),
            arr_delay_causes: HashMap::new(),
            otp: BTreeMap::new(),
        };
        thread::spawn(move || proc.run())
    }

    fn run(mut self) -> MetricsProcessor {
        loop {
            let Ok(event) = self.receiver.recv() else {
                println!("metrics thread failed to receive event");
                panic!();
            };
            match event.data {
                ModelEventType::SimulationComplete => {
                    // TODO write data
                    // let model = self.model.upgrade().unwrap();
                    // let intervals = model.flights.iter()
                    //     .map(|(id, flight)| (id, flight.read().unwrap()))
                    //     .filter(|(_, flight)| !flight.cancelled)
                    //     .map(|(id, flight)| Interval {
                    //         start: flight.depart_time.unwrap().timestamp() as u64,
                    //         stop: flight.arrive_time.unwrap().timestamp() as u64,
                    //         val: *id
                    //     })
                    //     .collect::<Vec<_>>();
                    return self;
                }
                ModelEventType::SimulationStarted(model) => {
                    self.model = model;
                    let mdl = self.model.upgrade().unwrap();
                    self.arrival_delays.reserve(mdl.flights.len());
                    continue;
                }
                _ => {
                    println!("[{}] {:?}", event.time, event.data);
                }
            }

            self.track_otp(&event);
            self.track_delay_causes(&event);
        }
    }

    fn track_otp(&mut self, event: &ModelEvent) {
        if let ModelEventType::FlightArrived(id) = event.data {
            let Some(mdl) = self.model.upgrade() else {
                return;
            };
            let flt = mdl.flight_read(id);
            // println!("[{}] {:?} ({}, {} from {} to {} with {} passengers, piloted by {})",
            //     event.time, event.data, &flt.flight_number, &flt.aircraft_tail, &flt.origin, &flt.dest, flt.passengers.iter().map(|i| i.count).sum::<u32>(), flt.crew[0]);
            let delay = max(TimeDelta::zero(), event.time - flt.sched_arrive);
            self.arrival_delays.push(delay.num_minutes() as u16);

            let mut prev = self.otp
                .last_key_value()
                .map(|i| i.1)
                .unwrap_or(&(0, 0, 0)).clone();
            prev.1 += 1;
            if delay.num_minutes() <= 15 {
                prev.0 += 1;
            }
            self.otp.insert(event.time, prev);

            // println!(
            //     "passenger load report for flight {} from {} to {}:",
            //     flt.flight_number, flt.origin, flt.dest
            // );
            // for psg in &flt.passengers {
            //     println!(
            //         "{}\t of {} [history: {:?}]",
            //         psg.count,
            //         psg.path
            //             .iter()
            //             .map(|i| i.to_string())
            //             .collect::<Vec<String>>()
            //             .join("->"),
            //         &psg.flights_taken
            //     );
            // }
        } else if let ModelEventType::FlightCancelled(_, _) = event.data {
            let mut prev = self.otp
                .last_key_value()
                .map(|i| i.1)
                .unwrap_or(&(0, 0, 0)).clone();
            prev.2 += 1;
            self.otp.insert(event.time, prev);
        }
    }

    fn track_delay_causes(&mut self, event: &ModelEvent) {
        if let ModelEventType::FlightArrivalDelayed(_id, duration, reason) = &event.data {
            *self.arr_delay_causes.entry(reason.clone()).or_insert(0) +=
                duration.num_minutes() as u32;
        } else if let ModelEventType::FlightDepartureDelayed(_id, duration, reason) = &event.data {
            *self.dep_delay_causes.entry(reason.clone()).or_insert(0) +=
                duration.num_minutes() as u32;
        }
    }
}
