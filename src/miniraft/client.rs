use crate::miniraft::state::Command;

#[derive(Debug)]
pub struct ClientRequest {
    pub command: Command,
}
