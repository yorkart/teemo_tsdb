//#![feature(async_closure)]
#[macro_use]
extern crate log;
extern crate log4rs;

extern crate bytes;
extern crate futures;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tokio;

mod action;

use engine::Engine;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::sync::Arc;

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn action(
    req: Request<Body>,
    ts_engine: Arc<Box<dyn Engine + Send + Sync>>,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(Body::from("ok"))),

        (&Method::POST, "/search") => action::search(req, ts_engine).await,

        // Simply echo the body back to the client.
        (&Method::POST, "/append") => action::append(req, ts_engine).await,

        // Simply echo the body back to the client.
        (&Method::POST, "/table") => action::create_table(req, ts_engine).await,

        (&Method::POST, "/echo/reversed") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await?;

            let reversed_body = whole_body.iter().rev().cloned().collect::<Vec<u8>>();
            Ok(Response::new(Body::from(reversed_body)))
        }

        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

//#[tokio::main]
async fn serve0(
    ts_engine: Box<dyn Engine + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = ([0, 0, 0, 0], 8091).into();
    let ts_engine = Arc::new(ts_engine);
    let service = make_service_fn(|_conn| {
        let ts_engine_clone_1 = ts_engine.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |body| {
                let ts_engine_clone_2 = ts_engine_clone_1.clone();

                action(body, ts_engine_clone_2)
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);
    info!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}

pub fn serve(ts_engine: Box<dyn Engine + Send + Sync>) {
    let _ = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(serve0(ts_engine));
}
