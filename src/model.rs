use std::{collections::HashMap, sync::mpsc, thread::Thread};
use chrono::{DateTime, Utc};
use crate::{aircraft::Aircraft, airport::{Airport, AirportCode, Disruption}, crew::{Crew, CrewId}, metrics::ModelEvent};

pub struct Model<'a> {
    pub now: DateTime<Utc>,
    pub fleet: HashMap<String, Aircraft<'a>>,
    pub crew: HashMap<CrewId, Crew<'a>>,
    pub airports: HashMap<AirportCode, Airport<'a>>,
    pub disruptions: Vec<Box<dyn Disruption<'a>>>,
    pub publisher: mpsc::Sender<ModelEvent<'a>>,

    pub metrics_thread: Thread
}

impl<'a> std::fmt::Debug for Model<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Model {{ now={}, {} aircraft, {} crew, {} airports, {} disruptions }}",
            self.now, self.fleet.len(), self.crew.len(), self.airports.len(), self.disruptions.len()))
    }
}

// A model description file