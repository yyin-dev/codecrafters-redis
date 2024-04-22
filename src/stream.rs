use anyhow::{bail, Result};
use std::{collections::BTreeMap, fmt::Display};

const NOT_INCREASING_ERR_MSG: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";

const MIN_ID_ERR_MSG: &str = "ERR The ID specified in XADD must be greater than 0-0";

// Derived PartialEq and Eq is exactly what we want: compare `ms` and then `seq`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    ms: u64,
    seq: u64,
}

impl Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl EntryId {
    pub fn from_string(s: String) -> Result<Self> {
        let vs = s.split('-').collect::<Vec<_>>();
        if vs.len() != 2 {
            bail!("Expect 'ms-seq' format");
        }

        let ms = vs[0].to_string().parse()?;
        let seq = vs[1].to_string().parse()?;
        Ok(Self { ms, seq })
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct Stream {
    entries: BTreeMap<EntryId, Entry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn append(&mut self, entry_id: EntryId, entry: Entry) -> Result<()> {
        // Validate entry id is strictly increasing
        if entry_id <= (EntryId { ms: 0, seq: 0 }) {
            bail!(MIN_ID_ERR_MSG);
        }

        let max_entry = self.entries.iter().max_by_key(|e| e.0);
        if let Some((max_entry_id, _)) = max_entry {
            if &entry_id <= max_entry_id {
                bail!(NOT_INCREASING_ERR_MSG);
            }
        }

        self.entries.insert(entry_id, entry);
        Ok(())
    }
}
