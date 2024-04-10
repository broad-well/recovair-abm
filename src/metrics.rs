use chrono::{DateTime, Duration, Utc};

use crate::{aircraft::Flight, airport::AirportCode};

pub struct ModelEvent<'a> {
    time: DateTime<Utc>,
    data: ModelEventType<'a>
}

pub enum DelayReason {
    CrewShortage,
    AircraftShortage,
    RateLimited(AirportCode)
}

pub enum ModelEventType<'a> {
    // -- Flight lifecycle --
    FlightDepartureDelayed(&'a Flight, Duration, DelayReason),
    FlightCancelled(&'a Flight),
    FlightDeparted(&'a Flight),
    FlightArrivalDelayed(&'a Flight, Duration),
    FlightArrived(&'a Flight),

    // -- Aircraft stats --
    AircraftTurnedAround(&'a Aircraft, AirportCode, Duration),
    

    // -- Scheduled changes --
    CrewAssignmentChanged(&'a Flight),
    
}