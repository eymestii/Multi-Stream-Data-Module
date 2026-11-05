use std::time::Duration;

use prost_types::Timestamp;
use tokio_util::sync::CancellationToken;

use crate::proto::v1::{message_fragment::Kind, MessageFragment};

/// Convierte una fecha RFC 3339 (`2026-01-01T12:00:00Z`) al Timestamp de protobuf.
pub fn rfc3339_to_timestamp(value: &str) -> Option<Timestamp> {
    let dt = chrono::DateTime::parse_from_rfc3339(value).ok()?;
    Some(Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

/// Duerme `duration`, o termina antes si se cancela. Devuelve `false` si se canceló.
pub async fn sleep_or_cancel(duration: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => true,
        _ = cancel.cancelled() => false,
    }
}

pub fn text_fragment(text: impl Into<String>) -> MessageFragment {
    MessageFragment {
        kind: Some(Kind::Text(text.into())),
    }
}
