use std::{error::Error, fs::File, sync::Arc};

use chrono::{DateTime, Utc};
use csv::Writer;

use crate::model::Model;

pub fn export_finished_model(
    model: Arc<Model>,
    filename_prefix: &str,
) -> Result<(), Box<dyn Error>> {
    let mut flight_writer = Writer::from_path(format!("{}-flights.csv", filename_prefix))?;
    export_flights(&model, &mut flight_writer)?;
    Ok(())
}

fn export_flights(model: &Model, writer: &mut Writer<File>) -> Result<(), Box<dyn Error>> {
    writer.write_record(&[
        "id",
        "flight_number",
        "tail",
        "crew",
        "passengers",
        "origin",
        "dest",
        "cancelled",
        "dep_time",
        "arr_time",
        "sched_dep",
        "sched_arr",
    ])?;
    for flight in model.flights.values() {
        let flt = flight.read().unwrap();
        writer.write_record(&[
            &flt.id.to_string() as &str,
            &flt.flight_number.to_string(),
            flt.aircraft_tail.as_ref().unwrap_or(&String::new()),
            &flt.crew
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join(","),
            &flt.passengers
                .iter()
                .map(|p| p.count)
                .sum::<u32>()
                .to_string(),
            &format!("{}", flt.origin),
            &format!("{}", flt.dest),
            if flt.cancelled { "1" } else { "0" },
            &flt.depart_time
                .as_ref()
                .map(format_datetime)
                .unwrap_or(String::new()),
            &flt.arrive_time
                .as_ref()
                .map(format_datetime)
                .unwrap_or(String::new()),
            &format_datetime(&flt.sched_depart),
            &format_datetime(&flt.sched_arrive),
        ])?;
    }
    Ok(())
}

fn format_datetime(dt: &DateTime<Utc>) -> String {
    format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))
}
