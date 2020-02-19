use bytes::buf::BufExt;
use engine::Engine;
use hyper::{header, Body, Request, Response, StatusCode};
use serde_json::json;
use std::sync::Arc;

pub async fn create_table(
    req: Request<Body>,
    _ts_engine: Arc<Box<dyn Engine + Send + Sync>>,
) -> Result<Response<Body>, hyper::Error> {
    let whole_body = hyper::body::aggregate(req).await?;

    let data: serde_json::Value = serde_json::from_reader(whole_body.reader()).expect("");
    let json_map = data.as_object().unwrap();
    let _table_name = json_map.get("table_name").unwrap().as_str().unwrap();

    //    ts_engine.create_key(table_name.to_string());

    let resp_json = json!({
        "code": "200",
        "msg": "ok",
    });

    let json = resp_json.to_string(); // serde_json::to_string("ok").expect("");
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .expect("");
    Ok(response)
}
