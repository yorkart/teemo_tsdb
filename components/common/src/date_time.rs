use std::time::{SystemTime, UNIX_EPOCH, Duration};
use chrono::{DateTime, Utc, ParseError, FixedOffset};
use core::fmt;
use std::error::Error;

pub fn timestamp_to_interval_str(begin_time: u64, end_time: u64) -> String{
    format!("[{},{}), {}/{}",
             begin_time,
             end_time,
             timestamp_secs_to_string(begin_time),
             timestamp_secs_to_string(end_time))
}

pub fn now_timestamp_secs() -> u64{
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs()
}

/// format timestamp to string
///
pub fn timestamp_secs_to_string(timestamp: u64) -> String {
    let origin_dt: DateTime<Utc> = {
        let b_ts = UNIX_EPOCH + Duration::from_secs(timestamp);
        b_ts.into()
    };
    origin_dt.format("%Y-%m-%dT%T%z").to_string()
}

pub fn string_to_date_times(range_date_time: &str) -> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), DateTimeError> {
    let range : Vec<&str>= range_date_time.split('/').collect();
    if range.len() != 2 {
        return Err(DateTimeError::Invalid);
    }

    let fmt_str = "%Y-%m-%dT%T%z";
    let from = DateTime::parse_from_str(range.get(0).unwrap(), fmt_str);
    if from.is_err() {
        return Err(DateTimeError::ParseErr(from.err().unwrap()));
    }

    let to = DateTime::parse_from_str(range.get(1).unwrap(), fmt_str);
    if to.is_err() {
        return Err(DateTimeError::ParseErr(to.err().unwrap()));
    }

    Ok((from.unwrap(), to.unwrap()))
}

/// An error from the `parse` function.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum DateTimeError{
    /// Given field is out of permitted range.
    ParseErr(ParseError),

    /// The input string has some invalid character sequence for given formatting items.
    Invalid,
}

impl DateTimeError {
    pub fn description(&self) -> &str {
        match *self {
            DateTimeError::ParseErr(ref err) => err.description(),
            DateTimeError::Invalid => "input contains invalid characters",
        }
    }
}

impl From<ParseError> for DateTimeError {
    fn from(e: ParseError) -> Self {
        DateTimeError::ParseErr(e)
    }
}

impl fmt::Display for DateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.description().fmt(f)
    }
}

#[cfg(any(feature = "std", test))]
impl Error for DateTimeError {
    fn description(&self) -> &str {
        self.description()
    }
}
