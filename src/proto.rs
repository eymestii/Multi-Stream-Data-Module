//! Código generado a partir de `proto/streamchat.proto` y helpers sobre esos tipos.

pub mod v1 {
    tonic::include_proto!("streamchat.v1");
}

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("streamchat_descriptor");

use v1::{chat_event::Payload, ChatEvent, Platform};

/// Nombre corto de la plataforma, usado en filtros como `twitch:canal`.
pub fn platform_key(platform: i32) -> &'static str {
    match Platform::try_from(platform) {
        Ok(Platform::Twitch) => "twitch",
        Ok(Platform::Kick) => "kick",
        _ => "unknown",
    }
}

/// Nombre del tipo de evento, igual al nombre del campo en el `oneof payload`.
pub fn event_type_name(event: &ChatEvent) -> &'static str {
    match &event.payload {
        Some(Payload::Message(_)) => "message",
        Some(Payload::MessageDeleted(_)) => "message_deleted",
        Some(Payload::ChatCleared(_)) => "chat_cleared",
        Some(Payload::UserBanned(_)) => "user_banned",
        Some(Payload::Follow(_)) => "follow",
        Some(Payload::Subscription(_)) => "subscription",
        Some(Payload::GiftSubscription(_)) => "gift_subscription",
        Some(Payload::Raid(_)) => "raid",
        Some(Payload::Cheer(_)) => "cheer",
        Some(Payload::StreamStatus(_)) => "stream_status",
        Some(Payload::RewardRedemption(_)) => "reward_redemption",
        None => "unknown",
    }
}
