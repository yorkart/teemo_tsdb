//#![feature(async_closure)]

extern crate bytes;
extern crate futures;
extern crate tokio;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode, header};
use bytes::buf::BufExt;
use tsz::storage::mut_mem::BTreeEngine;
use std::sync::mpsc::SyncSender;
use tsz::DataPoint;
use std::borrow::Borrow;

//struct HttpServer {
//    ts_map: Arc<RwLock<TSMap>>,
//}

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn echo(req: Request<Body>, ts_map: BTreeEngine, tx : SyncSender<DataPoint>) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(Body::from(
            "Try POSTing data to /echo such as: `curl localhost:3000/echo -XPOST -d 'hello world'`",
        ))),

        // Simply echo the body back to the client.
        (&Method::POST, "/append") => {
            let d1 = DataPoint::new(1482268055 + 10, 1.24);
            let _a = tx.send(d1);

            let whole_body = hyper::body::aggregate(req).await?;
            let _hh = ts_map.get(String::from("abc").borrow());

            let data: serde_json::Value = serde_json::from_reader(whole_body.reader()).expect("");

            let json = serde_json::to_string(&data).expect("");
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json)).expect("");
            Ok(response)
        }

        // Reverse the entire body before sending back to the client.
        //
        // Since we don't know the end yet, we can't simply stream
        // the chunks as they arrive as we did with the above uppercase endpoint.
        // So here we do `.await` on the future, waiting on concatenating the full body,
        // then afterwards the content can be reversed. Only then can we return a `Response`.
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
async fn serve0(ts_map: BTreeEngine, tx : SyncSender<DataPoint>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = ([0, 0, 0, 0], 8091).into();

    let service = make_service_fn(|_conn| {
        let ts_map_clone_1 = ts_map.clone();
        let tx_clone_1 = tx.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |body| {
                let ts_map_clone_2 = ts_map_clone_1.clone();
                let tx_clone_2 = tx_clone_1.clone();

                echo(body, ts_map_clone_2, tx_clone_2)
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);
    println!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}

pub fn serve(ts_map: BTreeEngine, tx : SyncSender<DataPoint>) {
    let _ = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(serve0(ts_map, tx));
}
