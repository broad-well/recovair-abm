use crate::{
    aircraft::{Aircraft, Flight, FlightId},
    airport::{Airport, AirportCode, Clearance, Disruption, DisruptionIndex},
    crew::{Crew, CrewId},
    metrics::{CancelReason, MetricsProcessor, ModelEvent, ModelEventType},
};
use chrono::{DateTime, TimeDelta, Utc};
use neon::types::Finalize;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    thread::JoinHandle,
};

#[derive(Debug)]
pub struct ModelConfig {
    pub crew_turnaround_time: TimeDelta,
    pub aircraft_turnaround_time: TimeDelta,
    pub max_delay: TimeDelta,
}

// Model should never be mutably borrowed as it needs to be borrowed practically everywhere
pub struct Model {
    pub _now: Arc<RwLock<DateTime<Utc>>>,
    pub end: DateTime<Utc>,
    pub fleet: HashMap<String, Arc<RwLock<Aircraft>>>,
    pub crew: HashMap<CrewId, Arc<RwLock<Crew>>>,
    pub airports: HashMap<AirportCode, Arc<RwLock<Airport>>>,
    pub flights: HashMap<FlightId, Arc<RwLock<Flight>>>,
    pub disruptions: DisruptionIndex,
    pub publisher: mpsc::Sender<ModelEvent>,

    pub metrics: RwLock<Option<JoinHandle<MetricsProcessor>>>,
    pub config: ModelConfig,
}

impl Finalize for Model {}

// This is a macro because if it were a member function,
// we'd run into problems with concurrent borrows of self
// (from experience)
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

impl Model {
    pub fn now(&self) -> DateTime<Utc> {
        self._now.read().unwrap().clone()
    }

    pub fn flight_write(&self, id: FlightId) -> RwLockWriteGuard<'_, Flight> {
        self.flights.get(&id).unwrap().write().unwrap()
    }
    pub fn flight_read(&self, id: FlightId) -> RwLockReadGuard<'_, Flight> {
        self.flights.get(&id).unwrap().read().unwrap()
    }

    /// Make the given flight depart from its origin, i.e., transition from Scheduled to Enroute.
    pub fn depart_flight(&self, flight_id: FlightId) {
        let now = self.now();
        let mut flight = self.flights.get(&flight_id).unwrap().write().unwrap();
        let mut aircraft = self
            .fleet
            .get(flight.aircraft_tail.as_ref().unwrap())
            .unwrap()
            .write()
            .unwrap();
        // This sends AircraftTurnedAround
        send_event!(self, aircraft.takeoff(flight_id, self.now()));
        for crew_id in &flight.crew {
            self.crew
                .get(crew_id)
                .unwrap()
                .write()
                .unwrap()
                .takeoff(&flight);
        }
        flight.takeoff(now);
        let mut origin = self.airports.get(&flight.origin).unwrap().write().unwrap();
        origin.mark_departure(self.now(), &mut flight, aircraft.type_.1);
        send_event!(self, ModelEventType::FlightDeparted(flight_id));
    }

    /// Make the given flight arrive at its destination, i.e., transition from Enroute to Scheduled.
    pub fn arrive_flight(&self, flight_id: FlightId) {
        // Update: Flight, resources (Aircraft, Crew)
        let mut flight = self.flights.get(&flight_id).unwrap().write().unwrap();
        {
            let mut aircraft = self
                .fleet
                .get(flight.aircraft_tail.as_ref().unwrap())
                .unwrap()
                .write()
                .unwrap();
            aircraft.land(flight.dest, self.now());
        }
        for crew_id in &flight.crew {
            self.crew
                .get(crew_id)
                .unwrap()
                .write()
                .unwrap()
                .land(&flight, self.now());
        }
        {
            flight.land(self.now());
            let mut dest = self.airports.get(&flight.dest).unwrap().write().unwrap();
            dest.mark_arrival(self.now(), &flight);
        }
        send_event!(self, ModelEventType::FlightArrived(flight_id))
    }

    pub fn cancel_flight(&self, flight_id: FlightId, reason: CancelReason) {
        let mut flt = self.flight_write(flight_id);
        flt.cancelled = true;
        for crew in &flt.crew {
            self.crew[crew].write().unwrap().unclaim(flight_id);
        }
        if let Some(tail) = &flt.aircraft_tail {
            self.fleet[tail]
                .write()
                .unwrap()
                .unclaim(flight_id);
        }
        send_event!(self, ModelEventType::FlightCancelled(flight_id, reason));
    }

    pub fn request_departure(
        &self,
        flight_id: FlightId,
    ) -> Option<(Clearance, Vec<(Arc<RwLock<dyn Disruption>>, TimeDelta)>)> {
        let flt = self.flights.get(&flight_id).unwrap();
        let disruptions = self.disruptions.lookup(&flt.read().unwrap());

        if let Some((earliest, reason_breakdown)) = self.reserve_earliest(
            &disruptions,
            Vec::new(),
            &self.now(),
            None,
            |dis: Arc<RwLock<dyn Disruption>>, time: &DateTime<Utc>|
                dis.write().unwrap().request_depart(&flt.read().unwrap(), self, time),
            |dis: Arc<RwLock<dyn Disruption>>, time: &DateTime<Utc>|
                dis.write().unwrap().void_depart_clearance(&flt.read().unwrap(), time, self)) {
            
            if earliest <= self.now() {
                Some((Clearance::Cleared, Vec::new()))
            } else {
                Some((Clearance::EDCT(earliest), reason_breakdown))
            }
        } else { None }
    }

    pub fn request_arrival(
        &self,
        flight_id: FlightId,
    ) -> Option<(Clearance, Vec<(Arc<RwLock<dyn Disruption>>, TimeDelta)>)> {
        let flt = self.flights.get(&flight_id).unwrap();
        let disruptions = self.disruptions.lookup(&flt.read().unwrap());

        if let Some((earliest, reason_breakdown)) = self.reserve_earliest(
            &disruptions,
            Vec::new(),
            &self.now(),
            None,
            |dis: Arc<RwLock<dyn Disruption>>, time: &DateTime<Utc>|
                dis.write().unwrap().request_arrive(&flt.read().unwrap(), self, time),
            |dis: Arc<RwLock<dyn Disruption>>, time: &DateTime<Utc>|
                dis.write().unwrap().void_arrive_clearance(&flt.read().unwrap(), time, self)) {

            if earliest <= self.now() {
                Some((Clearance::Cleared, Vec::new()))
            } else {
                Some((Clearance::EDCT(earliest), reason_breakdown))
            }
        } else { None }
    }

    /// Find the earliest time to execute a flight action that is accepted by all disruptions
    fn reserve_earliest<F, V>(&self, disruptions: &Vec<Arc<RwLock<dyn Disruption>>>, mut reasons: Vec<(Arc<RwLock<dyn Disruption>>, TimeDelta)>, starting: &DateTime<Utc>, already_slotted: Option<usize>, request: F, void: V) -> Option<(DateTime<Utc>, Vec<(Arc<RwLock<dyn Disruption>>, TimeDelta)>)>
        where F: Fn(Arc<RwLock<dyn Disruption>>, &DateTime<Utc>) -> Clearance, V: Fn(Arc<RwLock<dyn Disruption>>, &DateTime<Utc>) {
        let mut cleared_indices: Vec<usize> = Vec::with_capacity(disruptions.len());
        for (i, disruption) in disruptions.iter().enumerate() {
            if Some(i) != already_slotted {
                if let Some(later) = request(disruption.clone(), starting).time() {
                    if *later > *starting {
                        for index in cleared_indices {
                            void(disruptions[index].clone(), starting);
                        }
                        if *later >= self.now() + self.config.max_delay {
                            println!("reserve_earliest exceeded max_delay! later={} reasons={:?}", later, &reasons);
                            return None;
                        }
                        reasons.push((disruption.clone(), *later - *starting));
                        return self.reserve_earliest(disruptions, reasons, later, Some(i), request, void);
                    }
                }
            }
            cleared_indices.push(i);
        }
        debug_assert_eq!(cleared_indices.len(), disruptions.len());
        Some((*starting, reasons))
    }
}

impl std::fmt::Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Model {{ now={}, {} aircraft, {} crew, {} airports, {} disruptions }}",
            self.now(),
            self.fleet.len(),
            self.crew.len(),
            self.airports.len(),
            self.disruptions.len()
        ))
    }
}
