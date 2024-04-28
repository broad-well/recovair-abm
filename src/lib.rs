extern crate chrono;
use std::sync::Arc;

use metrics::MetricsProcessor;
use model::Model;
use neon::prelude::*;
use scenario::{ScenarioLoader, SqliteScenarioLoader};

mod aircraft;
mod airport;
mod crew;
mod dispatcher;
mod metrics;
mod model;
mod scenario;
mod export;

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
    };
}

struct FinishedModel {
    model: Arc<Model>,
    metrics: MetricsProcessor
}

impl Finalize for FinishedModel {}

fn encode_model(mut cx: FunctionContext) -> JsResult<JsObject> {
    let finished_model = &cx.argument::<JsBox<FinishedModel>>(0)?;
    let model = &finished_model.model;
    let flights = cx.empty_object();
    for (flight_id, flight) in &model.flights {
        let flight = {
            let flight = flight.read().unwrap();
            let obj = cx.empty_object();
            if !flight.cancelled {
                if let Some(depart_time) = flight.depart_time {
                    object_set!(
                        cx,
                        obj,
                        "start",
                        cx.number(depart_time.timestamp() as f64 * 1000f64)
                    );
                }
                if let Some(arrive_time) = flight.arrive_time {
                    object_set!(
                        cx,
                        obj,
                        "end",
                        cx.number(arrive_time.timestamp() as f64 * 1000f64)
                    );
                }
            }
            object_set!(
                cx,
                obj,
                "sched_start",
                cx.number(flight.sched_depart.timestamp() as f64 * 1000f64)
            );
            object_set!(
                cx,
                obj,
                "sched_end",
                cx.number(flight.sched_arrive.timestamp() as f64 * 1000f64)
            );
            object_set!(cx, obj, "origin", cx.string(flight.origin.to_string()));
            object_set!(cx, obj, "dest", cx.string(flight.dest.to_string()));
            object_set!(
                cx,
                obj,
                "flight_number",
                cx.string(flight.flight_number.to_string())
            );
            if let Some(tail) = flight.aircraft_tail.clone() {
                object_set!(cx, obj, "tail", cx.string(tail));
            } else {
                object_set!(cx, obj, "tail", cx.null());
            }
            object_set!(cx, obj, "cancelled", cx.boolean(flight.cancelled));
            Ok(obj)
        }?;
        object_set!(cx, flights, flight_id.to_string().as_str(), flight);
    }
    let fleet = cx.empty_object();
    for (tail, aircraft) in &model.fleet {
        let kind = { cx.string(aircraft.read().unwrap().type_.0.clone()) };
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
    let metrics = {
        let arrival_delay_dist = {
            let arr = cx.empty_array();
            for (i, delay) in finished_model.metrics.arrival_delays.iter().enumerate() {
                object_set!(cx, arr, i as u32, cx.number(*delay as i32));
            }
            arr
        };
        let otp = {
            let obj = cx.empty_object();
            for (time, (on_time, total, cancelled)) in &finished_model.metrics.otp {
                let arr = cx.empty_array();
                object_set!(cx, arr, 0, cx.number(*on_time));
                object_set!(cx, arr, 1, cx.number(*total));
                object_set!(cx, arr, 2, cx.number(*cancelled));
                object_set!(cx, obj, (time.timestamp() * 1000).to_string().as_str(), arr);
            }
            obj
        };
        let dep_delay_reasons = cx.empty_object();
        for (reason, minutes) in finished_model.metrics.dep_delay_causes.iter() {
            object_set!(cx, dep_delay_reasons, format!("{:?}", reason).as_str(), cx.number(*minutes));
        }
        let arr_delay_reasons = cx.empty_object();
        for (reason, minutes) in finished_model.metrics.arr_delay_causes.iter() {
            object_set!(cx, arr_delay_reasons, format!("{:?}", reason).as_str(), cx.number(*minutes));
        }
        
        let obj = cx.empty_object();
        obj.set(&mut cx, "delays", arrival_delay_dist)?;
        obj.set(&mut cx, "otp", otp)?;
        obj.set(&mut cx, "dep_delay_reasons", dep_delay_reasons)?;
        obj.set(&mut cx, "arr_delay_reasons", arr_delay_reasons)?;
        obj
    };

    let obj = cx.empty_object();
    obj.set(&mut cx, "flights", flights)?;
    obj.set(&mut cx, "fleet", fleet)?;
    obj.set(&mut cx, "demands", demands)?;
    obj.set(&mut cx, "metrics", metrics)?;
    Ok(obj)
}

fn run_model(mut cx: FunctionContext) -> JsResult<JsBox<FinishedModel>> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);
    let scenario = cx.argument::<JsString>(1)?.value(&mut cx);
    let loader = try_load!(&mut cx, SqliteScenarioLoader::new(&path, scenario));
    let model = Arc::new(try_load!(&mut cx, loader.read_model()));
    let mut dispatcher = try_load!(&mut cx, loader.read_dispatcher(model.clone()));

    dispatcher.init_flight_updates();
    dispatcher.run_model();
    let Some(handle) = model.metrics.write().unwrap().take() else { panic!() };
    let metrics = handle.join().expect("Metrics thread failed");

    Ok(cx.boxed(FinishedModel { model, metrics }))
}

fn export_csvs(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let model = cx.argument::<JsBox<FinishedModel>>(0)?;
    let prefix = cx.argument::<JsString>(1)?.value(&mut cx);


    if let Err(err) = export::export_finished_model(model.model.clone(), &prefix) {
        return cx.throw_error(err.to_string());
    }
    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("runModel", run_model)?;
    cx.export_function("readModel", encode_model)?;
    cx.export_function("exportModel", export_csvs)?;
    Ok(())
}
