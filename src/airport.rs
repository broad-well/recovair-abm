use std::collections::LinkedList;
use crate::{aircraft::Aircraft, crew::Crew, model::Model};

pub type AirportCode = [u8; 3];
#[derive(Debug)]
pub struct Airport<'a> {
    pub code: AirportCode,
    pub model: &'a Model<'a>,
    pub fleet: LinkedList<&'a Aircraft<'a>>,
    pub crew: LinkedList<&'a Crew<'a>>
}

pub struct PassengerDemand {
    pub path: Vec<AirportCode>,
    pub count: u32
}

/// Group of passengers transported
pub struct PassengerGroup<'a> {
    pub path: &'a [AirportCode],
    pub count: u32
}