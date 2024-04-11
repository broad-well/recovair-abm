use crate::{
    aircraft::{Aircraft, Flight, FlightId},
    airport::{Airport, AirportCode, Disruption},
    crew::{Crew, CrewId},
    metrics::{ModelEvent, ModelEventType},
};
use chrono::{DateTime, TimeDelta, Utc};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    thread::Thread,
};

#[derive(Debug)]
pub struct ModelConfig {
    pub crew_turnaround_time: TimeDelta,
    pub aircraft_turnaround_time: TimeDelta,
    pub max_delay: TimeDelta,
}

pub struct Model {
    pub now: DateTime<Utc>,
    pub fleet: HashMap<String, Arc<RwLock<Aircraft>>>,
    pub crew: HashMap<CrewId, Arc<RwLock<Crew>>>,
    pub airports: HashMap<AirportCode, Arc<RwLock<Airport>>>,
    pub flights: HashMap<FlightId, Arc<RwLock<Flight>>>,
    pub disruptions: Vec<Arc<dyn Disruption>>,
    pub publisher: mpsc::Sender<ModelEvent>,

    pub metrics_thread: Thread,
    pub config: ModelConfig,
}

// This is a macro because if it were a member function,
// we'd run into problems with concurrent borrows of self
// (from experience)
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

impl Model {
    pub fn flight_write(&self, id: FlightId) -> RwLockWriteGuard<'_, Flight> {
        self.flights.get(&id).unwrap().write().unwrap()
    }
    pub fn flight_read(&self, id: FlightId) -> RwLockReadGuard<'_, Flight> {
        self.flights.get(&id).unwrap().read().unwrap()
    }

    /// Make the given flight depart from its origin, i.e., transition from Scheduled to Enroute.
    pub fn depart_flight(&mut self, flight_id: u64) {
        let now = self.now;
        {
            let mut flight = self.flight_write(flight_id);
            flight.takeoff(now);
        }
        let flight = self.flights.get(&flight_id).unwrap().read().unwrap();
        {
            let mut aircraft = self
                .fleet
                .get(&flight.aircraft_tail)
                .unwrap()
                .write()
                .unwrap();
            // This sends AircraftTurnedAround
            send_event!(self, aircraft.takeoff(flight_id, self.now));
        }
        for crew_id in &flight.crew {
            self.crew
                .get_mut(crew_id)
                .unwrap()
                .write()
                .unwrap()
                .takeoff(flight_id);
        }
        send_event!(
            self,
            ModelEventType::FlightDeparted(self.flights.get(&flight_id).unwrap().clone())
        );
    }

    /// Make the given flight arrive at its destination, i.e., transition from Enroute to Scheduled.
    pub fn arrive_flight(&mut self, flight_id: u64) {
        // Update: Flight, resources (Aircraft, Crew)
        {
            let mut flight = self.flight_write(flight_id);
            flight.land(self.now);
        }

        let flight = self.flights.get(&flight_id).unwrap().read().unwrap();
        {
            let mut aircraft = self
                .fleet
                .get(&flight.aircraft_tail)
                .unwrap()
                .write()
                .unwrap();
            aircraft.land(flight.dest, self.now);
        }
        for crew_id in &flight.crew {
            self.crew.get(crew_id).unwrap().write().unwrap().land();
        }
        send_event!(
            self,
            ModelEventType::FlightArrived(self.flights.get(&flight_id).unwrap().clone())
        )
    }
}

impl std::fmt::Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Model {{ now={}, {} aircraft, {} crew, {} airports, {} disruptions }}",
            self.now,
            self.fleet.len(),
            self.crew.len(),
            self.airports.len(),
            self.disruptions.len()
        ))
    }
}
