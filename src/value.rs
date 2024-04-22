#[derive(Clone, Debug)]
pub enum Value {
    String(String),
}

impl Value {
    pub fn type_string(&self) -> String {
        "string".into()
    }

    pub fn to_string(&self) -> String {
        let Self::String(s) = self;
        s.clone()
    }
}
