extern crate chrono;
use std::sync::Arc;

use dispatcher::Dispatcher;
use neon::prelude::*;
use scenario::{ScenarioLoader, SqliteScenarioLoader};

pub mod aircraft;
pub mod airport;
pub mod crew;
pub mod dispatcher;
pub mod metrics;
pub mod model;
pub mod scenario;

fn hello(mut cx: FunctionContext) -> JsResult<JsString> {
    Ok(cx.string("hello node"))
}

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

fn run_model(mut cx: FunctionContext) -> JsResult<JsString> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);
    let scenario = cx.argument::<JsString>(1)?.value(&mut cx);
    let loader = try_load!(&mut cx, SqliteScenarioLoader::new(&path, scenario));
    let model = Arc::new(try_load!(&mut cx, loader.read_model()));
    let mut dispatcher = try_load!(&mut cx, loader.read_dispatcher(model.clone()));

    dispatcher.run_model();
    if let Some(handle) = model.metrics.write().unwrap().take() {
        handle.join().expect("Metrics thread failed");
    }

    Ok(cx.string("done"))
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("hello", hello)?;
    cx.export_function("runModel", run_model)?;
    Ok(())
}
