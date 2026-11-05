//! Traducción de eventos de EventSub al modelo común.

use serde::Deserialize;
use serde_json::{from_value, Value};

use super::Metadata;
use crate::proto::v1::{
    chat_event::Payload, message_fragment::Kind, Badge, Channel, ChatEvent, ChatMessage, Cheermote,
    Emote, Mention, MessageDeleted, MessageFragment, Platform, ReplyInfo, StreamStatus, User,
};
use crate::util::{rfc3339_to_timestamp, text_fragment};

pub(super) fn normalize(sub_type: &str, meta: &Metadata, event: &Value) -> Option<ChatEvent> {
    let broadcaster: Broadcaster = from_value(event.clone()).ok()?;

    let payload = match sub_type {
        "channel.chat.message" => Payload::Message(chat_message(from_value(event.clone()).ok()?)),
        "channel.chat.message_delete" => {
            #[derive(Deserialize)]
            struct Deleted {
                message_id: String,
            }
            let d: Deleted = from_value(event.clone()).ok()?;
            Payload::MessageDeleted(MessageDeleted {
                message_id: d.message_id,
                moderator: None,
            })
        }
        "stream.online" | "stream.offline" => Payload::StreamStatus(StreamStatus {
            is_live: sub_type == "stream.online",
            ..Default::default()
        }),
        _ => return None,
    };

    Some(ChatEvent {
        platform: Platform::Twitch as i32,
        platform_event_id: meta.message_id.clone(),
        channel: Some(broadcaster.into_channel()),
        occurred_at: rfc3339_to_timestamp(&meta.message_timestamp),
        payload: Some(payload),
        raw_json: event.to_string(),
        ..Default::default()
    })
}

#[derive(Deserialize)]
struct Broadcaster {
    broadcaster_user_id: String,
    #[serde(default)]
    broadcaster_user_login: String,
    #[serde(default)]
    broadcaster_user_name: String,
}

impl Broadcaster {
    fn into_channel(self) -> Channel {
        Channel {
            platform: Platform::Twitch as i32,
            id: self.broadcaster_user_id,
            slug: self.broadcaster_user_login,
            display_name: self.broadcaster_user_name,
        }
    }
}

#[derive(Deserialize)]
struct ChatMessageEvent {
    chatter_user_id: String,
    chatter_user_login: String,
    chatter_user_name: String,
    message_id: String,
    message: MessageBody,
    #[serde(default)]
    color: String,
    #[serde(default)]
    badges: Vec<BadgeEvent>,
    reply: Option<ReplyEvent>,
}

#[derive(Deserialize)]
struct MessageBody {
    text: String,
    #[serde(default)]
    fragments: Vec<FragmentEvent>,
}

#[derive(Deserialize)]
struct FragmentEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: String,
    cheermote: Option<CheermoteEvent>,
    emote: Option<EmoteEvent>,
    mention: Option<MentionEvent>,
}

#[derive(Deserialize)]
struct CheermoteEvent {
    prefix: String,
    bits: i32,
    tier: i32,
}

#[derive(Deserialize)]
struct EmoteEvent {
    id: String,
}

#[derive(Deserialize)]
struct MentionEvent {
    user_id: String,
    user_login: String,
}

#[derive(Deserialize)]
struct BadgeEvent {
    set_id: String,
    id: String,
}

#[derive(Deserialize)]
struct ReplyEvent {
    parent_message_id: String,
    #[serde(default)]
    parent_message_body: String,
    #[serde(default)]
    parent_user_id: String,
    #[serde(default)]
    parent_user_login: String,
    #[serde(default)]
    parent_user_name: String,
}

fn chat_message(e: ChatMessageEvent) -> ChatMessage {
    let has_badge = |set: &str| e.badges.iter().any(|b| b.set_id == set);
    let sender = User {
        id: e.chatter_user_id.clone(),
        username: e.chatter_user_login.clone(),
        display_name: e.chatter_user_name.clone(),
        color: e.color.clone(),
        is_broadcaster: has_badge("broadcaster"),
        is_moderator: has_badge("moderator"),
        is_vip: has_badge("vip"),
        is_subscriber: has_badge("subscriber") || has_badge("founder"),
        badges: e
            .badges
            .iter()
            .map(|b| Badge {
                r#type: b.set_id.clone(),
                version: b.id.clone(),
                image_url: String::new(),
            })
            .collect(),
    };

    let fragments = e.message.fragments.into_iter().map(fragment).collect();

    ChatMessage {
        message_id: e.message_id,
        sender: Some(sender),
        text: e.message.text,
        fragments,
        reply: e.reply.map(|r| ReplyInfo {
            parent_message_id: r.parent_message_id,
            parent_text: r.parent_message_body,
            parent_sender: Some(User {
                id: r.parent_user_id,
                username: r.parent_user_login,
                display_name: r.parent_user_name,
                ..Default::default()
            }),
        }),
    }
}

fn fragment(f: FragmentEvent) -> MessageFragment {
    let kind = match (f.kind.as_str(), f.emote, f.mention, f.cheermote) {
        ("emote", Some(emote), _, _) => Kind::Emote(Emote {
            image_url: format!(
                "https://static-cdn.jtvnw.net/emoticons/v2/{}/default/dark/3.0",
                emote.id
            ),
            id: emote.id,
            name: f.text,
        }),
        ("mention", _, Some(m), _) => Kind::Mention(Mention {
            user_id: m.user_id,
            username: m.user_login,
        }),
        ("cheermote", _, _, Some(c)) => Kind::Cheermote(Cheermote {
            prefix: c.prefix,
            bits: c.bits,
            tier: c.tier,
        }),
        _ => return text_fragment(f.text),
    };
    MessageFragment { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::super::Envelope;
    use super::*;

    const CHAT_FIXTURE: &str = include_str!("../../../fixtures/twitch/channel.chat.message.json");

    #[test]
    fn normaliza_mensaje_de_chat() {
        let env: Envelope = serde_json::from_str(CHAT_FIXTURE).unwrap();
        let ev = normalize("channel.chat.message", &env.metadata, &env.payload["event"]).unwrap();

        assert_eq!(ev.platform, Platform::Twitch as i32);
        assert_eq!(ev.platform_event_id, env.metadata.message_id);
        assert_eq!(ev.channel.as_ref().unwrap().slug, "micanal");
        assert!(ev.occurred_at.is_some());

        let Some(Payload::Message(m)) = ev.payload else { panic!("no es un mensaje") };
        let sender = m.sender.unwrap();
        assert_eq!(sender.username, "espectador");
        assert!(sender.is_moderator);
        assert!(sender.is_subscriber);
        assert_eq!(m.text, "hola @micanal Kappa");
        assert!(matches!(m.fragments[1].kind, Some(Kind::Mention(_))));
        let Some(Kind::Emote(emote)) = &m.fragments[3].kind else { panic!("no es emote") };
        assert_eq!(emote.name, "Kappa");
        assert!(emote.image_url.contains("/25/"));
    }

    #[test]
    fn ignora_tipos_desconocidos() {
        let env: Envelope = serde_json::from_str(CHAT_FIXTURE).unwrap();
        assert!(normalize("channel.algo_nuevo", &env.metadata, &env.payload["event"]).is_none());
    }
}
