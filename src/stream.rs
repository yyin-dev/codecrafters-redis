use anyhow::{bail, Result};
use std::{collections::BTreeMap, fmt::Display};

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
        // TODO: validate entry_id
        self.entries.insert(entry_id, entry);
        Ok(())
    }
}
