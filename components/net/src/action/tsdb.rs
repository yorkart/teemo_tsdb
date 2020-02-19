use bytes::buf::BufExt;
use engine::{Engine, Raw};
use hyper::{header, Body, Request, Response, StatusCode};
use serde_json::json;
use std::borrow::Borrow;
use std::sync::Arc;
use tszv1::{DataPoint, Decode};

pub async fn search(
    req: Request<Body>,
    ts_engine: Arc<Box<dyn Engine + Send + Sync>>,
) -> Result<Response<Body>, hyper::Error> {
    let whole_body = hyper::body::aggregate(req).await?;

    let data: serde_json::Value = serde_json::from_reader(whole_body.reader()).expect("");
    let json_map = data.as_object().unwrap();
    let table_name = json_map.get("table_name").unwrap().as_str().unwrap();
    let key = json_map.get("key").unwrap().as_str().unwrap();
    let interval = json_map.get("interval").unwrap().as_str().unwrap();
    let limit = match json_map.get("limit") {
        Some(v) => v.as_i64().unwrap() as usize,
        None => 0,
    };

    let resp_json = match common::string_to_date_times(interval) {
        Ok((from, to)) => {
            let resp_data =
                match ts_engine.get(table_name.to_string().borrow(), key.to_string().borrow()) {
                    Some(ts) => {
                        let from = from.timestamp() as u64;
                        let to = to.timestamp() as u64;

                        let list = ts.get_decoder(from, to, limit, |mut decoder, dp_vec| {
                            loop {
                                match decoder.next() {
                                    Ok(dp) => {
                                        dp_vec.push(dp);
                                        //                                    info!("reader => {}, {}", dp.time, dp.value);
                                    }
                                    Err(_) => {
                                        break;
                                    }
                                }
                            }
                            // list
                        });
                        list
                    }
                    None => Vec::new(),
                };
            json!({
                "code": "200",
                "msg": "",
                "data": resp_data,
            })
        }
        Err(err) => json!({
            "code": "500",
            "msg": err.description(),
        }),
    };

    let json = resp_json.to_string(); // serde_json::to_string("ok").expect("");
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .expect("");
    Ok(response)
}

pub async fn append(
    req: Request<Body>,
    ts_engine: Arc<Box<dyn Engine + Send + Sync>>,
) -> Result<Response<Body>, hyper::Error> {
    let whole_body = hyper::body::aggregate(req).await?;

    let data: serde_json::Value = serde_json::from_reader(whole_body.reader()).expect("");
    let json_map = data.as_object().unwrap();
    let table_name = json_map.get("table_name").unwrap().as_str().unwrap();
    let key = json_map.get("key").unwrap().as_str().unwrap();
    let mut timestamp = json_map.get("timestamp").unwrap().as_u64().unwrap();
    let mut value = json_map.get("value").unwrap().as_f64().unwrap();

    if timestamp == 0 {
        timestamp = common::now_timestamp_secs();
    }
    if value < 0f64 {
        value = (timestamp % 100) as f64;
    }

    ts_engine.append(Raw {
        table_name: String::from(table_name),
        key: String::from(key),
        data_point: DataPoint::new(timestamp, value),
    });

    let resp_json = json!({
        "code": "200",
        "msg": "ok",
    });
    let json = resp_json.to_string();
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .expect("");
    Ok(response)
}
