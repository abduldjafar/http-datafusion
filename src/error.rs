use serde::Serialize;
use tracing::error as log_error;


pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize)]
pub enum Error {
    DataFusionError(String),
    YamlError(String),
    IoError(String),


}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}

macro_rules! impl_from_error {
    ($($type:ty => $variant:ident),* $(,)?) => {
        $(impl From<$type> for Error {
            fn from(error: $type) -> Self {
                log_error!("{}", error);
                Error::$variant(error.to_string())
            }
        })*
    };
}

impl_from_error!(
    datafusion::error::DataFusionError => DataFusionError,
    serde_yaml::Error => YamlError,
    std::io::Error => IoError,
);