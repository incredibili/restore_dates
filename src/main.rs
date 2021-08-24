use chrono::{DateTime, NaiveDate, Utc};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::fmt::{Error, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::{env, fmt, fs, io};

fn main() {
    let args: Vec<String> = env::args().collect();
    let path = &args[1];

    let mut organizations: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut intermediari: HashMap<String, DateTime<Utc>> = HashMap::new();

    fs::read_dir(path)
        .expect("Unable to read folder")
        .map(|res| {
            res.map(|e| {
                let bytes: Vec<u8> = fs::read(e.path())
                    .expect(format!("Unable to read file {:?}", e.file_name()).as_str())
                    .into_iter()
                    .collect();

                //let file_string = String::from_utf8(bytes).unwrap().into_bytes();

                let mut reader = csv::ReaderBuilder::new()
                    .delimiter(b';')
                    .from_reader(&bytes[..]);
                let csv_result: Vec<CsvLine> = reader
                    .deserialize()
                    .collect::<Result<Vec<CsvLine>, csv::Error>>()
                    .unwrap();

                let date_from_filename = &e.file_name().into_string().unwrap()[0..=7];

                let year = &date_from_filename[0..=3];
                let month = &date_from_filename[4..=5];
                let day = &date_from_filename[6..=7];
                let final_date = format!("{}-{}-{}T00:00:00.000000+00:00", year, month, day);

                println!(
                    "Processing File: {:?}, timestamp: {:?}",
                    e.file_name(),
                    final_date
                );

                let activated_at_for_file = DateTime::parse_from_rfc3339(&final_date)
                    .unwrap()
                    .with_timezone(&Utc);

                csv_result.into_iter().for_each(|csv_item| {
                    if intermediari.contains_key(&String::from(csv_item.email.as_str())) {
                        println!(
                            "Same intermediario email {:?} found twice",
                            csv_item.email.as_str()
                        )
                    }

                    if intermediari
                        .get(csv_item.email.as_str())
                        .filter(|&d| *d < activated_at_for_file)
                        .is_none()
                    {
                        intermediari.insert(csv_item.email.into(), activated_at_for_file);
                    }
                });
            })
        })
        .collect::<Result<Vec<_>, io::Error>>()
        .unwrap();

    println!("INTERMEDIARI: {:?}", intermediari);

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

impl From<SanitizedString> for String {
    fn from(str: SanitizedString) -> Self {
        String::from(str.0)
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
