use std::{cell::RefCell, rc::Rc};

use mio::{net::TcpStream, Token};

/// master keeps track of current replication offset of each connected replica.
/// At each write, it updates the values of each offset. On a wait, it compares the updated
/// value with the current value for each replica and only sends a getack if needed. If not, it
/// does not need to send a wait.
// Here, for simplication, we only keep track of whether the replica stream is uptodate.
// On any write command to master, we set up_to_date top false
#[derive(Debug)]
pub struct Replica {
    pub stream: Rc<RefCell<TcpStream>>,
    pub up_to_date: bool,
    pub token: Token,
}

impl Replica {
    pub fn new(stream: TcpStream, token: Token) -> Self {
        Self {
            stream: Rc::new(RefCell::new(stream)),
            up_to_date: true,
            token,
        }
    }
}
