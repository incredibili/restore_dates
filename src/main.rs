use chrono::NaiveDate;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt::{Error, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::{env, fmt, fs, io};

fn main() {
    let args: Vec<String> = env::args().collect();
    let path = &args[1];

    let mut entries = fs::read_dir(path)
        .unwrap()
        .map(|res| {
            res.map(|e| {
                let bytes: Vec<u8> = fs::read(e.path()).unwrap().into_iter().collect();
                //let file_string = String::from_utf8(bytes).unwrap().into_bytes();

                let mut reader = csv::ReaderBuilder::new()
                    .delimiter(b';')
                    .from_reader(&bytes[..]);
                let csv_result: Vec<CsvLine> = reader
                    .deserialize()
                    .collect::<Result<Vec<CsvLine>, csv::Error>>()
                    .unwrap();

                println!(
                    "File: {:?}, numero records: {:?}",
                    e.file_name(),
                    csv_result.len()
                );
            })
        })
        .collect::<Result<Vec<_>, io::Error>>()
        .unwrap();

    //entries.sort();
}

#[derive(Deserialize, Clone, Debug)]
struct CsvLine {
    #[serde(rename = "Email")]
    pub email: SanitizedString,
    #[serde(rename = "Inizio")]
    pub start_date: NaiveDate,
    // YYYY-MM-dd
    #[serde(rename = "Email Primaria")]
    #[serde(deserialize_with = "de_from_si_no")]
    pub is_primary_email: bool,
    #[serde(rename = "Anagrafica Intermediario")]
    pub anagrafica_intermediario: SanitizedString,
    #[serde(rename = "Anagrafica Organizzazione")]
    pub anagrafica_organization: SanitizedString,
    #[serde(rename = "Nome Organizzazione")]
    pub organization_name: SanitizedString,
    #[serde(rename = "Convenzione")]
    pub convention: String,
    #[serde(rename = "RUI Organizzazione")]
    pub rui: SanitizedString,
    #[serde(rename = "Ruolo")]
    pub role: CsvIntermediarioRole,
    #[serde(rename = "Attivo")]
    #[serde(deserialize_with = "de_from_si_no")]
    pub active: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
enum CsvIntermediarioRole {
    #[serde(alias = "admin")]
    #[serde(alias = "Admin")]
    Admin,
    #[serde(alias = "intermediario")]
    #[serde(alias = "Intermediario")]
    Intermediario,
}

fn de_from_si_no<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.to_uppercase().as_str() {
        "SI" => Ok(true),
        "NO" => Ok(false),
        _ => Err(serde::de::Error::custom(format!(
            "Cannot deserialize {} to boolean",
            s
        ))),
    }
}

#[derive(Debug, Clone)]
struct SanitizedString(String);

impl SanitizedString {
    fn new(v: &str) -> Self {
        Self(v.trim().to_lowercase().into())
    }
}

impl AsRef<str> for SanitizedString {
    fn as_ref(&self) -> &str {
        &*self.0
    }
}

impl Deref for SanitizedString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for SanitizedString {
    fn from(str: String) -> Self {
        SanitizedString::new(&str)
    }
}

impl FromStr for SanitizedString {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SanitizedString::new(s))
    }
}

impl From<&str> for SanitizedString {
    fn from(str: &str) -> Self {
        SanitizedString::new(str)
    }
}

impl fmt::Display for SanitizedString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for SanitizedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SanitizedStringVisitor)
    }
}

struct SanitizedStringVisitor;

impl<'de> Visitor<'de> for SanitizedStringVisitor {
    type Value = SanitizedString;

    fn expecting(&self, formatter: &mut Formatter) -> Result<(), Error> {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SanitizedString::new(v))
    }
}
