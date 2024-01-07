use colored::{Color, Colorize};
use common::util;
use flate2::{write::GzEncoder, Compression};
use log::*;
use log4rs::{
    append::{
        rolling_file::{
            policy::compound::{roll::Roll, trigger::size::SizeTrigger, CompoundPolicy},
            RollingFileAppender,
        },
        Append,
    },
    config::{Appender, Config, Root},
    encode::{self, Encode},
    filter::{Filter, Response},
};
use rustyline::ExternalPrinter;
use std::io::Write;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    fs::{read_dir, remove_file, rename, File},
    io::{self, Cursor},
    path::{Component, Path, PathBuf},
    sync::Mutex,
    thread,
};
use time::OffsetDateTime;

const FILE_SIZE_LIMIT: u64 = 50_000_000;

macro_rules! format_record {
    ($record:expr) => {{
        let record = $record;
        let location = Location::from_record(record);
        format!(
            "[{} {}{}{}]: {}",
            format_time(current_time()),
            record.metadata().level(),
            if matches!(location, Location::Some { .. }) {
                " "
            } else {
                ""
            },
            location,
            record.args()
        )
    }};
}

macro_rules! write_record {
    ($writer:expr, $record:expr, $color:expr) => {{
        let writer = $writer;
        let record = $record;
        let color = $color;
        writeln!(writer, "{}", format_record!(record).color(color))
    }};

    ($writer:expr, $record:expr) => {{
        let writer = $writer;
        let record = $record;
        writeln!(writer, "{}", format_record!(record))
    }};
}

// Sets up log4rs customized for the minecraft server
pub fn init_logger<P: ExternalPrinter + Send + 'static>(
    mut printer: P,
) -> Result<(), anyhow::Error> {
    printer.print("\n".to_owned())?;

    // Logs info to the console with colors and such
    let console = CustomConsoleAppender {
        printer: Mutex::new(printer),
    };

    // Logs to log files
    let log_file = RollingFileAppender::builder()
        .encoder(Box::new(LogEncoder))
        .build(
            "logs/latest.log",
            Box::new(CompoundPolicy::new(
                Box::new(SizeTrigger::new(FILE_SIZE_LIMIT)),
                Box::new(CustomLogRoller::new()),
            )),
        )?;

    // Build the log4rs config
    let config = Config::builder()
        .appender(
            Appender::builder()
                .filter(Box::new(CrateFilter))
                .build("console", Box::new(console)),
        )
        .appender(
            Appender::builder()
                .filter(Box::new(CrateFilter))
                .build("log_file", Box::new(log_file)),
        )
        .build(
            Root::builder()
                .appender("console")
                .appender("log_file")
                .build(common::config::Config::get().log_level_filter),
        )?;

    log4rs::init_config(config)?;

    Ok(())
}

// Called at the end of main, compresses the last log file
pub fn cleanup() {
    // There's no reason to handle an error here
    let _ = CustomLogRoller::new().roll_threaded(Path::new("./logs/latest.log"), false);
}

fn current_time() -> OffsetDateTime {
    common::config::Config::localize(OffsetDateTime::now_utc())
}

fn format_time(datetime: OffsetDateTime) -> String {
    match datetime.format(&*util::TIME_FORMAT) {
        Ok(formatted) => formatted,
        Err(_) => "??:??:??".to_owned(),
    }
}

// Only allow logging from out crate
#[derive(Debug)]
struct CrateFilter;

impl Filter for CrateFilter {
    fn filter(&self, record: &Record) -> Response {
        match record.module_path() {
            Some(path) => {
                if ["common", "entity", "engine", "history", "rest"]
                    .iter()
                    .any(|&krate| path.starts_with(krate))
                {
                    Response::Accept
                } else {
                    Response::Reject
                }
            }
            None => Response::Reject,
        }
    }
}

// Custom implementation for a console logger so that it doesn't mangle the user's commands
struct CustomConsoleAppender<P> {
    printer: Mutex<P>,
}

impl<P: ExternalPrinter + Send + 'static> Append for CustomConsoleAppender<P> {
    fn append(&self, record: &Record) -> Result<(), anyhow::Error> {
        let mut writer = Cursor::new(Vec::<u8>::new());

        let color = match record.metadata().level() {
            Level::Error => Color::Red,
            Level::Warn => Color::Yellow,
            Level::Debug => Color::BrightCyan,
            Level::Trace => Color::BrightMagenta,
            _ => Color::White,
        };

        write_record!(&mut writer, record, color)?;

        self.printer.lock().unwrap().print(
            String::from_utf8(writer.into_inner())
                .unwrap_or_else(|error| format!("failed to format string: {error}")),
        )?;

        Ok(())
    }

    fn flush(&self) {}
}

impl<P> Debug for CustomConsoleAppender<P> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomConsoleAppender {{ .. }}")
    }
}

#[derive(Debug)]
struct CustomLogRoller {
    name_info: Mutex<(u16, u32)>, // current day, log count for today
}

impl CustomLogRoller {
    pub fn new() -> Self {
        let mut max_index = 0;

        if let Ok(paths) = read_dir("./logs/") {
            let today = current_time()
                .format(&*util::DATE_FORMAT)
                .unwrap_or_default();

            // Find the logs that match today's date and determine the highest index ({date}-{index}.log).
            for path in paths
                .flatten()
                .flat_map(|entry| entry.file_name().into_string())
                .filter(|name| name.starts_with(&today))
            {
                if let Some(index) = Self::index_from_path(&path) {
                    if index > max_index {
                        max_index = index;
                    }
                }
            }
        }

        CustomLogRoller {
            name_info: Mutex::new((current_time().ordinal(), max_index)),
        }
    }

    fn index_from_path(path: &str) -> Option<u32> {
        let dash_index = path.rfind('-')?;
        let dot_index = path.find('.')?;
        path.get(dash_index.saturating_add(1)..dot_index)
            .and_then(|index| index.parse::<u32>().ok())
    }

    pub fn roll_threaded(&self, file: &Path, threaded: bool) -> Result<(), anyhow::Error> {
        let mut guard = match self.name_info.lock() {
            Ok(g) => g,

            // Since the mutex is privately managed and errors are handled correctly, this shouldn't be an issue
            Err(_) => unreachable!("Logger mutex poisoned."),
        };

        // Check to make sure the log name info is still accurate
        let local_datetime = current_time();
        if local_datetime.ordinal() != guard.0 {
            guard.0 = local_datetime.ordinal();
            guard.1 = 1;
        } else {
            guard.1 = guard.1.wrapping_add(1);
        }

        // Rename the file in case it's large and will take a while to compress
        let log = "./logs/latest-tmp.log";
        rename(file, log)?;

        let output = format!(
            "./logs/{}-{}.log.gz",
            local_datetime.format(&*util::DATE_FORMAT)?,
            guard.1
        );

        drop(guard);

        if threaded {
            thread::spawn(move || {
                Self::try_compress_log(log, &output);
            });
        } else {
            Self::try_compress_log(log, &output);
        }

        Ok(())
    }

    // Attempts compress_log and prints an error if it fails
    fn try_compress_log(input_path: &str, output_path: &str) {
        if let Err(error) = Self::compress_log(Path::new(input_path), Path::new(output_path)) {
            error!("Failed to compress log file: {error:?}");
        }
    }

    // Takes the source file and compresses it, writing to the output path. Removes the source when done.
    fn compress_log(input_path: &Path, output_path: &Path) -> Result<(), io::Error> {
        let mut input = File::open(input_path)?;
        let mut output = GzEncoder::new(File::create(output_path)?, Compression::default());
        io::copy(&mut input, &mut output)?;
        drop(output.finish()?);
        drop(input); // This needs to occur before file deletion on some OS's
        remove_file(input_path)
    }
}

impl Roll for CustomLogRoller {
    fn roll(&self, file: &Path) -> Result<(), anyhow::Error> {
        self.roll_threaded(file, true)
    }
}

#[derive(Debug)]
struct LogEncoder;

impl Encode for LogEncoder {
    fn encode(&self, writer: &mut dyn encode::Write, record: &Record<'_>) -> anyhow::Result<()> {
        write_record!(writer, record).map_err(Into::into)
    }
}

enum Location<'a> {
    None,
    Some { file: Cow<'a, str>, line: u32 },
}

impl<'a> Location<'a> {
    fn from_record(record: &Record<'a>) -> Self {
        let (file, line) = match record.level() {
            Level::Info | Level::Warn => return Self::None,
            _ => match (record.file(), record.line()) {
                (Some(file), Some(line)) => (file, line),
                _ => return Self::None,
            },
        };

        let truncated = Path::new(file)
            .components()
            .skip_while(|component| {
                matches!(
                    component,
                    Component::Prefix(_)
                        | Component::RootDir
                        | Component::CurDir
                        | Component::ParentDir
                ) || component == &Component::Normal("src".as_ref())
            })
            .collect::<PathBuf>();
        match truncated.into_os_string().into_string() {
            Ok(string) => Self::Some {
                file: Cow::Owned(string),
                line,
            },
            Err(_) => Self::Some {
                file: Cow::Borrowed(file),
                line,
            },
        }
    }
}

impl<'a> Display for Location<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Some { file, line } => write!(f, "{}:{}", file, line),
        }
    }
}
