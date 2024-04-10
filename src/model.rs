use std::{collections::HashMap, sync::mpsc};
use chrono::{DateTime, Utc};
use crate::{aircraft::Aircraft, airport::{Airport, AirportCode}, crew::{Crew, CrewId}};

#[derive(Debug)]
pub struct Model<'a> {
    pub now: DateTime<Utc>,
    pub fleet: HashMap<String, Aircraft<'a>>,
    pub crew: HashMap<CrewId, Crew<'a>>,
    pub airports: HashMap<AirportCode, Airport<'a>>
}