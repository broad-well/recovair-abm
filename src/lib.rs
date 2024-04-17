extern crate chrono;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use model::Model;
use neon::prelude::*;
use scenario::{ScenarioLoader, SqliteScenarioLoader};

pub mod aircraft;
pub mod airport;
pub mod crew;
pub mod dispatcher;
pub mod metrics;
pub mod model;
pub mod scenario;


macro_rules! try_load {
    ( $cx:expr, $load_op:expr ) => {{
        let maybe = $load_op;
        match maybe {
            Err(error) => {
                let message = format!("Failed to load scenario: {:?}", error);
                return $cx.throw_error(message);
            }
            Ok(value) => value,
        }
    }};
}

macro_rules! object_set {
    ( $cx:expr, $obj:expr, $key:expr, $val:expr ) => {
        let item = $val;
        // Can't mutably borrow $cx multiple times at once, hence the above statement
        $obj.set(&mut $cx, $key, item)?;
    }
}

fn encode_model(mut cx: FunctionContext) -> JsResult<JsObject> {
    let model = cx.argument::<JsBox<Arc<Model>>>(0)?;
    let flights = cx.empty_object();
    for (flight_id, flight) in &model.flights {
        let flight = {
            let flight = flight.read().unwrap();
            let obj = cx.empty_object();
            if !flight.cancelled {
                object_set!(cx, obj, "start", cx.date(flight.depart_time.unwrap().timestamp() as f64 * 1000f64).expect("bad date"));
                object_set!(cx, obj, "end", cx.date(flight.arrive_time.unwrap().timestamp() as f64 * 1000f64).expect("bad date"));
            }
            object_set!(cx, obj, "origin", cx.string(flight.origin.to_string()));
            object_set!(cx, obj, "dest", cx.string(flight.dest.to_string()));
            object_set!(cx, obj, "flight_number", cx.string(flight.flight_number.to_string()));
            object_set!(cx, obj, "tail", cx.string(flight.aircraft_tail.to_string()));
            object_set!(cx, obj, "cancelled", cx.boolean(flight.cancelled));
            Ok(obj)
        }?;
        object_set!(cx, flights, flight_id.to_string().as_str(), flight);
    }
    let fleet = cx.empty_object();
    for (tail, aircraft) in &model.fleet {
        let kind = {
            cx.string(aircraft.read().unwrap().type_.0.clone())
        };
        object_set!(cx, fleet, tail.as_str(), kind);
    }
    let demands = cx.empty_object();
    for (loc, airport) in &model.airports {
        let airport = airport.read().unwrap();
        let value = cx.empty_array();
        for (i, demand) in airport.passengers.iter().enumerate() {
            let obj = cx.empty_object();
            let path = cx.empty_array();
            for (i, code) in demand.path.iter().enumerate() {
                object_set!(cx, path, i as u32, cx.string(code.to_string()));
            }
            object_set!(cx, obj, "path", path);
            object_set!(cx, obj, "count", cx.number(demand.count));
            let flights = cx.empty_array();
            for (i, flight) in demand.flights_taken.iter().enumerate() {
                object_set!(cx, flights, i as u32, cx.number(*flight as u32));
            }
            object_set!(cx, obj, "flights", flights);
            object_set!(cx, value, i as u32, obj);
        }
        object_set!(cx, demands, loc.to_string().as_str(), value);
    }

    
    let obj = cx.empty_object();
    obj.set(&mut cx, "flights", flights)?;
    obj.set(&mut cx, "fleet", fleet)?;
    obj.set(&mut cx, "demands", demands)?;
    Ok(obj)
}

fn run_model(mut cx: FunctionContext) -> JsResult<JsBox<Arc<Model>>> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);
    let scenario = cx.argument::<JsString>(1)?.value(&mut cx);
    let loader = try_load!(&mut cx, SqliteScenarioLoader::new(&path, scenario));
    let model = Arc::new(try_load!(&mut cx, loader.read_model()));
    let mut dispatcher = try_load!(&mut cx, loader.read_dispatcher(model.clone()));

    dispatcher.init_flight_updates();
    dispatcher.run_model();
    if let Some(handle) = model.metrics.write().unwrap().take() {
        handle.join().expect("Metrics thread failed");
    }

    Ok(cx.boxed(model))
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("runModel", run_model)?;
    cx.export_function("readModel", encode_model)?;
    Ok(())
}
