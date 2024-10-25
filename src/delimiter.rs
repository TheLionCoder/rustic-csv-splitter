use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Delimiter {
    Comma,
    Pipe,
    Tab,
    SemiColon,
}

impl Delimiter {
    pub const COMMA: u8 = b',';
    pub const PIPE: u8 = b'|';
    pub const TAB: u8 = b'\t';
    pub const SEMICOLON: u8 = b';';
}

impl fmt::Display for Delimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: String = match self {
            Delimiter::Comma => String::from(","),
            Delimiter::Pipe => String::from("|"),
            Delimiter::Tab => String::from("\t"),
            Delimiter::SemiColon => String::from(";"),
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Error)]
pub enum DelimiterParseError {
    #[error("Invalid delimiter")]
    InvalidDelimiter,
}

impl FromStr for Delimiter {
    type Err = DelimiterParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "," => Ok(Delimiter::Comma),
            "|" => Ok(Delimiter::Pipe),
            "\t" => Ok(Delimiter::Tab),
            ";" => Ok(Delimiter::SemiColon),
            _ => Err(DelimiterParseError::InvalidDelimiter),
        }
    }
}

impl From<Delimiter> for u8 {
    fn from(val: Delimiter) -> Self {
        match val {
            Delimiter::Comma => Delimiter::COMMA,
            Delimiter::Pipe => Delimiter::PIPE,
            Delimiter::Tab => Delimiter::TAB,
            Delimiter::SemiColon => Delimiter::SEMICOLON,
        }
    }
}
