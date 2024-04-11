use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};

use crate::{
    aircraft::Flight,
    airport::{AirportCode, Disruption},
    crew::CrewId,
};

pub struct ModelEvent {
    pub time: DateTime<Utc>,
    pub data: ModelEventType,
}

#[derive(Debug, Clone)]
pub enum DelayReason {
    CrewShortage,
    AircraftShortage,
    RateLimited(AirportCode, Arc<dyn Disruption>),
}

#[derive(Debug, Clone)]
pub enum ModelEventType {
    // -- Flight lifecycle --
    // Sender: Dispatcher
    // (A Flight object does not change once arrived)
    FlightDepartureDelayed(Arc<RwLock<Flight>>, Duration, DelayReason),
    FlightCancelled(Arc<RwLock<Flight>>),
    FlightDeparted(Arc<RwLock<Flight>>),
    FlightArrivalDelayed(Arc<RwLock<Flight>>, Duration),
    FlightArrived(Arc<RwLock<Flight>>),

    // -- Aircraft stats --
    // Sender: Dispatcher
    AircraftTurnedAround(String, AirportCode, Duration),

    // -- Scheduled changes --
    // Sender: Dispatcher
    CrewAssignmentChanged(Arc<RwLock<Flight>>),
    AircraftAssignmentChanged(Arc<RwLock<Flight>>),

    // -- Decision points --
    // Sender: Dispatcher
    CrewSelection(String, Vec<CrewId>),
    /// (Flight number, tail numbers to choose from)
    AircraftSelection(String, Vec<String>),

    // -- Completion --
    SimulationComplete,
}
