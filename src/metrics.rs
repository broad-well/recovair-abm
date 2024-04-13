use std::sync::{Arc, RwLock, Weak};

use chrono::{DateTime, Duration, Utc};

use crate::{
    aircraft::{Flight, FlightId},
    airport::{AirportCode, Disruption},
    crew::CrewId,
    model::Model,
};

pub struct ModelEvent {
    pub time: DateTime<Utc>,
    pub data: ModelEventType,
}

#[derive(Debug, Clone)]
pub enum DelayReason {
    CrewShortage,
    AircraftShortage,
    Disrupted(Weak<RwLock<dyn Disruption>>),
    RateLimited(AirportCode),
}

#[derive(Debug, Clone)]
pub enum ModelEventType {
    // -- Flight lifecycle --
    // Sender: Dispatcher
    // (A Flight object does not change once arrived)
    FlightDepartureDelayed(FlightId, Duration, DelayReason),
    FlightCancelled(FlightId),
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
