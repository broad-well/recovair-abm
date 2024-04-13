use crate::{
    aircraft::{Aircraft, Flight, FlightId},
    airport::{Airport, AirportCode, Clearance, Disruption},
    crew::{Crew, CrewId},
    metrics::{ModelEvent, ModelEventType},
};
use chrono::{DateTime, TimeDelta, Utc};
use std::{
    cell::Cell,
    collections::HashMap,
    rc::Weak,
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
    pub disruptions: Vec<Arc<RwLock<dyn Disruption>>>,
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
    pub fn depart_flight(&self, flight_id: FlightId) {
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
        let flight = self.flights.get(&flight_id).unwrap().read().unwrap();
        for crew_id in &flight.crew {
            self.crew
                .get(crew_id)
                .unwrap()
                .write()
                .unwrap()
                .takeoff(&flight);
        }
        {
            let mut origin = self.airports.get(&flight.origin).unwrap().write().unwrap();
            origin.mark_departure(self.now);
        }
        send_event!(self, ModelEventType::FlightDeparted(flight_id));
    }

    /// Make the given flight arrive at its destination, i.e., transition from Enroute to Scheduled.
    pub fn arrive_flight(&self, flight_id: FlightId) {
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
            self.crew
                .get(crew_id)
                .unwrap()
                .write()
                .unwrap()
                .land(&flight, self.now);
        }
        {
            let mut dest = self.airports.get(&flight.dest).unwrap().write().unwrap();
            dest.mark_arrival(self.now);
        }
        send_event!(self, ModelEventType::FlightArrived(flight_id))
    }

    pub fn cancel_flight(&self, flight_id: FlightId) {
        let mut flt = self.flight_write(flight_id);
        flt.cancelled = true;
        send_event!(self, ModelEventType::FlightCancelled(flight_id));
    }

    pub fn request_departure(
        &self,
        flight_id: FlightId,
    ) -> (Clearance, Option<Arc<RwLock<dyn Disruption>>>) {
        let flt = self.flights.get(&flight_id).unwrap();
        let effective_disruption = self
            .disruptions
            .iter()
            .map(|disruption| {
                (
                    disruption.clone(),
                    disruption
                        .write()
                        .unwrap()
                        .request_depart(flt.read().unwrap()),
                )
            })
            .max_by(|a, b| a.1.cmp(&b.1));

        if let Some((disruption, clearance)) = effective_disruption {
            (clearance, Some(disruption))
        } else {
            (Clearance::Cleared, None)
        }
    }

    // TODO reduce duplication
    pub fn request_arrival(
        &self,
        flight_id: FlightId,
    ) -> (Clearance, Option<Arc<RwLock<dyn Disruption>>>) {
        let flt = self.flights.get(&flight_id).unwrap();
        let effective_disruption = self
            .disruptions
            .iter()
            .map(|disruption| {
                (
                    disruption.clone(),
                    disruption
                        .write()
                        .unwrap()
                        .request_arrive(flt.read().unwrap()),
                )
            })
            .max_by(|a, b| a.1.cmp(&b.1));

        if let Some((disruption, clearance)) = effective_disruption {
            (clearance, Some(disruption))
        } else {
            (Clearance::Cleared, None)
        }
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
