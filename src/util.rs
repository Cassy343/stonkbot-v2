use once_cell::sync::Lazy;
use time::{
    format_description::{self, FormatItem},
    OffsetDateTime,
};

use crate::config::Config;

pub const SECONDS_TO_DAYS: i64 = 24 * 60 * 60;

pub static TIME_FORMAT: Lazy<Vec<FormatItem<'static>>> = Lazy::new(|| {
    format_description::parse("[hour repr:24]:[minute]:[second]")
        .expect("Invalid time format description")
});

pub static DATE_FORMAT: Lazy<Vec<FormatItem<'static>>> =
    Lazy::new(|| format_description::parse("[year]-[month]-[day]").expect("Invalid date format"));

pub fn localize(datetime: OffsetDateTime) -> OffsetDateTime {
    datetime.to_offset(Config::get().utc_offset.get())
}
