extern crate log;
extern crate log4rs;

fn main() {
    init_log();

    let engine = engine::create_engine("b-tree").unwrap();
    net::serve(engine);
}

fn parse_arg(arg_key: String) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    for arg in args.iter() {
        let a: String = arg.to_string();
        let tokens: Vec<&str> = a.split("=").collect();
        if tokens.len() != 2 {
            continue;
        }

        let key = tokens.get(0).expect("");
        if key.to_string().eq(arg_key.as_str()) {
            let value = tokens.get(1).expect("");
            return Option::Some(value.to_string());
        }
    }

    return Option::None;
}

fn init_log() {
    let key_conf = "config_path".to_string();
    let config_path = parse_arg(key_conf).expect("can not find [config_path] key in path");

    let log_config_path = config_path + "/log4rs.yaml";
    println!("log config path: {}", log_config_path);
    log4rs::init_file(log_config_path, Default::default()).unwrap();
}
