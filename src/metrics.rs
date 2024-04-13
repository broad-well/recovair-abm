use std::{
    path::Path,
    sync::{mpsc, Weak},
    thread::{self, JoinHandle},
};

use chrono::{DateTime, Duration, Utc};

use crate::{aircraft::FlightId, airport::AirportCode, crew::CrewId, model::Model};

pub struct ModelEvent {
    pub time: DateTime<Utc>,
    pub data: ModelEventType,
}

#[derive(Debug, Clone)]
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
    CrewAssignmentChanged(FlightId),
    AircraftAssignmentChanged(FlightId),

    // -- Decision points --
    // Sender: Dispatcher
    CrewSelection(FlightId, Vec<CrewId>),
    AircraftSelection(FlightId, String),

    // -- Completion --
    SimulationComplete,
}

pub struct MetricsProcessor {
    receiver: mpsc::Receiver<ModelEvent>,
    model: Weak<Model>,
    // more memory needed to compute KPIs
}

pub struct MetricsReport {
    location: Path,
}

impl MetricsProcessor {
    pub fn new(receiver: mpsc::Receiver<ModelEvent>) -> JoinHandle<()> {
        let mut proc = Self {
            receiver,
            model: Weak::new(),
        };
        thread::spawn(move || proc.run())
    }

    fn run(&mut self) {
        for event in self.receiver.iter() {
            match event.data {
                ModelEventType::SimulationComplete => {
                    // TODO write data
                    break;
                }
                ModelEventType::SimulationStarted(model) => {
                    self.model = model;
                }
                _ => {
                    println!(
                        "[{}] {:?} ({})",
                        event.time,
                        event.data,
                        if self.model.upgrade().is_some() {
                            "model exists"
                        } else {
                            "model not found"
                        }
                    );
                }
            }
        }
    }
}
