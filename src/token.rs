use mio::Token;

// Standard way to accept commands
pub const SERVER: Token = Token(0);
// When registering from master, the associated connection is registered with this token

pub const MASTER: Token = Token(1);
// all others reserved for replicas

pub const FIRST_REPLICA_TOKEN: Token = Token(2);

pub const FIRST_UNIQUE_TOKEN: Token = Token(20);

#[derive(Debug, Clone)]
pub struct TokenTrack {
    unique_token: Token,
    replica_token: Token,
}

impl TokenTrack {
    pub fn new() -> Self {
        Self {
            // reserve others for replicas
            unique_token: FIRST_UNIQUE_TOKEN,
            replica_token: FIRST_REPLICA_TOKEN,
        }
    }

    pub fn next_unique_token(&mut self) -> Token {
        let token = Token(self.unique_token.0);
        self.unique_token = Token(self.unique_token.0 + 1);
        token
    }

    pub fn next_replica_token(&mut self) -> Token {
        let token = Token(self.replica_token.0);
        self.replica_token = Token(self.replica_token.0 + 1);
        if self.replica_token.0 > FIRST_UNIQUE_TOKEN.0 {
            panic!("Nb of maximum replicas exceeded")
        }
        token
    }
}

impl Default for TokenTrack {
    fn default() -> Self {
        Self::new()
    }
}
