use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalMessage {
    Join {
        room_id: String,
        peer_id: String,
    },

    Joined {
        peer_id: String,
    },

    Offer {
        from: String,
        sdp: String,
    },

    Answer {
        from: String,
        sdp: String,
    },

    IceCandidate {
        from: String,
        candidate: IceCandidateData,
    },

    Leave {
        peer_id: String,
    },

    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCandidateData {
    pub candidate: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_mid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_mline_index: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub username_fragment: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl SignalMessage {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_and_deserializes_offer() {
        let message = SignalMessage::Offer {
            from: "client-a".into(),
            sdp: "test-sdp".into(),
        };

        let json = message.to_json().unwrap();

        let decoded = SignalMessage::from_json(&json).unwrap();

        match decoded {
            SignalMessage::Offer { from, sdp } => {
                assert_eq!(from, "client-a");
                assert_eq!(sdp, "test-sdp");
            }
            _ => panic!("Expected Offer"),
        }
    }

    #[test]
    fn serializes_ice_candidate() {
        let message = SignalMessage::IceCandidate {
            from: "client-a".into(),
            candidate: IceCandidateData {
                candidate: "...".into(),
                sdp_mid: Some("audio".into()),
                sdp_mline_index: Some(0),
                username_fragment: None,
                url: None,
            },
        };

        let json = message.to_json().unwrap();

        let decoded = SignalMessage::from_json(&json).unwrap();

        assert!(matches!(decoded, SignalMessage::IceCandidate { .. }));
    }
}

/// Versioned local signaling protocol. Relay membership is bound to the joined socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u8,
    pub generation: u64,
    #[serde(flatten)]
    pub body: WireMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMessage {
    JoinResumable {
        room: String,
        peer: String,
        token: String,
    },
    Joined {
        ticket: String,
    },
    Resume {
        room: String,
        peer: String,
        token: String,
        ticket: String,
    },
    Resumed {
        ticket: String,
    },
    Join {
        room: String,
        peer: String,
        token: String,
    },
    Waiting,
    Ready {
        remote: String,
        initiator: bool,
    },
    Offer {
        sdp: String,
    },
    Answer {
        sdp: String,
    },
    Ice {
        candidate: IceCandidateData,
    },
    Leave,
    Error {
        message: String,
    },
}

impl Envelope {
    pub fn new(generation: u64, body: WireMessage) -> Self {
        Self {
            version: 1,
            generation,
            body,
        }
    }
}
