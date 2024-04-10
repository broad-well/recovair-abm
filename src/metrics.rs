use chrono::{DateTime, Duration, Utc};

use crate::{
    aircraft::{Aircraft, Flight},
    airport::{AirportCode, Disruption},
    crew::CrewId,
};

pub struct ModelEvent<'a> {
    time: DateTime<Utc>,
    data: ModelEventType<'a>,
}

pub enum DelayReason<'a> {
    CrewShortage,
    AircraftShortage,
    RateLimited(AirportCode, &'a dyn Disruption<'a>),
}

pub enum ModelEventType<'a> {
    // -- Flight lifecycle --
    // Sender: Dispatcher
    FlightDepartureDelayed(&'a Flight<'a>, Duration, DelayReason<'a>),
    FlightCancelled(&'a Flight<'a>),
    FlightDeparted(&'a Flight<'a>),
    FlightArrivalDelayed(&'a Flight<'a>, Duration),
    FlightArrived(&'a Flight<'a>),

    // -- Aircraft stats --
    // Sender: Dispatcher
    AircraftTurnedAround(&'a Aircraft<'a>, AirportCode, Duration),

    // -- Scheduled changes --
    // Sender: Dispatcher
    CrewAssignmentChanged(&'a Flight<'a>),
    AircraftAssignmentChanged(&'a Flight<'a>),

    // -- Decision points --
    // Sender: Dispatcher
    CrewSelection(&'a str, Vec<CrewId>),
    /// (Flight number, tail numbers to choose from)
    AircraftSelection(&'a str, Vec<String>),
}
