#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    Pending,
    Done,
}

impl From<&Status> for &str {
    fn from(mode: &Status) -> Self {
        match mode {
            Status::Pending => "PENDING",
            Status::Done => "DONE",
        }
    }
}

impl TryFrom<&str> for Status {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "PENDING" => Ok(Status::Pending),
            "DONE" => Ok(Status::Done),
            s => Err(format!("Not a status: {}", s)),
        }
    }
}
