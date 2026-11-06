//! Traducción de los webhooks de Kick al modelo común.

use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;
use serde_json::{from_value, Value};

use crate::proto::v1::{
    chat_event::Payload, message_fragment::Kind, Badge, Channel, ChatEvent, ChatMessage, Emote,
    Follow, GiftSubscription, MessageFragment, Platform, ReplyInfo, RewardRedemption, StreamStatus,
    Subscription, User, UserBanned,
};
use crate::util::{rfc3339_to_timestamp, text_fragment};

/// Kick manda los emotes dentro del texto como `[emote:ID:NOMBRE]`.
static EMOTE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[emote:(\d+):([^\]]+)\]").expect("regex válida"));

pub(super) fn normalize(event_type: &str, message_id: &str, body: &Value) -> Option<ChatEvent> {
    #[derive(Deserialize)]
    struct Base {
        #[serde(default)]
        broadcaster: KickUser,
        created_at: Option<String>,
    }
    let base: Base = from_value(body.clone()).ok()?;

    let payload = match event_type {
        "chat.message.sent" => Payload::Message(chat_message(from_value(body.clone()).ok()?)),
        "channel.followed" => {
            #[derive(Deserialize)]
            struct Followed {
                #[serde(default)]
                follower: KickUser,
            }
            let f: Followed = from_value(body.clone()).ok()?;
            Payload::Follow(Follow {
                follower: Some(f.follower.to_user()),
            })
        }
        "channel.subscription.new" | "channel.subscription.renewal" => {
            #[derive(Deserialize)]
            struct Sub {
                #[serde(default)]
                subscriber: KickUser,
                #[serde(default)]
                duration: i32,
            }
            let s: Sub = from_value(body.clone()).ok()?;
            Payload::Subscription(Subscription {
                subscriber: Some(s.subscriber.to_user()),
                tier: "1".into(), // Kick tiene un solo nivel de suscripción
                cumulative_months: s.duration,
                is_renewal: event_type.ends_with("renewal"),
                ..Default::default()
            })
        }
        "channel.subscription.gifts" => {
            #[derive(Deserialize)]
            struct Gifts {
                #[serde(default)]
                gifter: KickUser,
                #[serde(default)]
                giftees: Vec<KickUser>,
            }
            let g: Gifts = from_value(body.clone()).ok()?;
            Payload::GiftSubscription(GiftSubscription {
                is_anonymous: g.gifter.is_anonymous,
                gifter: (!g.gifter.is_anonymous).then(|| g.gifter.to_user()),
                total: g.giftees.len() as i32,
                recipients: g.giftees.iter().map(KickUser::to_user).collect(),
                tier: "1".into(),
            })
        }
        "channel.reward.redemption.updated" => {
            #[derive(Deserialize)]
            struct Redemption {
                id: String,
                #[serde(default)]
                user_input: String,
                #[serde(default)]
                status: String,
                #[serde(default)]
                redeemer: KickUser,
                reward: Reward,
            }
            #[derive(Deserialize)]
            struct Reward {
                id: String,
                title: String,
                #[serde(default)]
                cost: i32,
            }
            let r: Redemption = from_value(body.clone()).ok()?;
            Payload::RewardRedemption(RewardRedemption {
                user: Some(r.redeemer.to_user()),
                reward_id: if r.reward.id.is_empty() { r.id } else { r.reward.id },
                reward_title: r.reward.title,
                cost: r.reward.cost,
                user_input: r.user_input,
                status: r.status,
            })
        }
        "livestream.status.updated" => {
            #[derive(Deserialize)]
            struct Live {
                is_live: bool,
                #[serde(default)]
                title: String,
            }
            let l: Live = from_value(body.clone()).ok()?;
            Payload::StreamStatus(StreamStatus {
                is_live: l.is_live,
                title: l.title,
                category: String::new(),
            })
        }
        "moderation.banned" => {
            #[derive(Deserialize)]
            struct Banned {
                #[serde(default)]
                moderator: KickUser,
                #[serde(default)]
                banned_user: KickUser,
                #[serde(default)]
                metadata: BanMetadata,
            }
            #[derive(Deserialize, Default)]
            struct BanMetadata {
                reason: String,
                expires_at: Option<String>,
            }
            let b: Banned = from_value(body.clone()).ok()?;
            Payload::UserBanned(UserBanned {
                user: Some(b.banned_user.to_user()),
                moderator: Some(b.moderator.to_user()),
                reason: b.metadata.reason,
                expires_at: b.metadata.expires_at.as_deref().and_then(rfc3339_to_timestamp),
            })
        }
        _ => return None,
    };

    Some(ChatEvent {
        platform: Platform::Kick as i32,
        platform_event_id: message_id.to_string(),
        channel: Some(base.broadcaster.to_channel()),
        occurred_at: base.created_at.as_deref().and_then(rfc3339_to_timestamp),
        payload: Some(payload),
        raw_json: body.to_string(),
        ..Default::default()
    })
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct KickUser {
    is_anonymous: bool,
    user_id: Option<u64>,
    username: String,
    channel_slug: String,
    identity: Option<Identity>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Identity {
    username_color: String,
    badges: Vec<KickBadge>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct KickBadge {
    #[serde(rename = "type")]
    kind: String,
    count: Option<i64>,
}

impl KickUser {
    fn to_user(&self) -> User {
        let badges = self.identity.as_ref().map(|i| i.badges.as_slice()).unwrap_or_default();
        let has = |kind: &str| badges.iter().any(|b| b.kind == kind);
        User {
            id: self.user_id.map(|id| id.to_string()).unwrap_or_default(),
            username: self.username.clone(),
            display_name: self.username.clone(),
            color: self
                .identity
                .as_ref()
                .map(|i| i.username_color.clone())
                .unwrap_or_default(),
            badges: badges
                .iter()
                .map(|b| Badge {
                    r#type: b.kind.clone(),
                    version: b.count.map(|c| c.to_string()).unwrap_or_default(),
                    image_url: String::new(),
                })
                .collect(),
            is_broadcaster: has("broadcaster"),
            is_moderator: has("moderator"),
            is_vip: has("vip"),
            is_subscriber: has("subscriber") || has("founder") || has("og"),
        }
    }

    fn to_channel(&self) -> Channel {
        Channel {
            platform: Platform::Kick as i32,
            id: self.user_id.map(|id| id.to_string()).unwrap_or_default(),
            slug: if self.channel_slug.is_empty() {
                self.username.to_lowercase()
            } else {
                self.channel_slug.clone()
            },
            display_name: self.username.clone(),
        }
    }
}

#[derive(Deserialize)]
struct ChatMessageEvent {
    message_id: String,
    #[serde(default)]
    sender: KickUser,
    #[serde(default)]
    content: String,
    replies_to: Option<RepliesTo>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RepliesTo {
    message_id: String,
    content: String,
    sender: KickUser,
}

fn chat_message(e: ChatMessageEvent) -> ChatMessage {
    let (text, fragments) = parse_content(&e.content);
    ChatMessage {
        message_id: e.message_id,
        sender: Some(e.sender.to_user()),
        text,
        fragments,
        reply: e.replies_to.map(|r| ReplyInfo {
            parent_message_id: r.message_id,
            parent_text: parse_content(&r.content).0,
            parent_sender: Some(r.sender.to_user()),
        }),
    }
}

/// Separa el contenido en fragmentos y devuelve también el texto plano
/// (con cada emote reemplazado por su nombre).
pub(super) fn parse_content(content: &str) -> (String, Vec<MessageFragment>) {
    let mut fragments = Vec::new();
    let mut plain = String::with_capacity(content.len());
    let mut last = 0;

    for caps in EMOTE_RE.captures_iter(content) {
        let whole = caps.get(0).expect("grupo 0 siempre existe");
        if whole.start() > last {
            let text = &content[last..whole.start()];
            plain.push_str(text);
            fragments.push(text_fragment(text));
        }
        let (id, name) = (&caps[1], &caps[2]);
        plain.push_str(name);
        fragments.push(MessageFragment {
            kind: Some(Kind::Emote(Emote {
                id: id.into(),
                name: name.into(),
                image_url: format!("https://files.kick.com/emotes/{id}/fullsize"),
            })),
        });
        last = whole.end();
    }
    if last < content.len() {
        let text = &content[last..];
        plain.push_str(text);
        fragments.push(text_fragment(text));
    }
    (plain, fragments)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAT_FIXTURE: &str = include_str!("../../../fixtures/kick/chat.message.sent.json");

    #[test]
    fn separa_texto_y_emotes() {
        let (plain, frags) = parse_content("hola [emote:37226:KEKW] mundo");
        assert_eq!(plain, "hola KEKW mundo");
        assert_eq!(frags.len(), 3);
        let Some(Kind::Emote(e)) = &frags[1].kind else { panic!("no es emote") };
        assert_eq!((e.id.as_str(), e.name.as_str()), ("37226", "KEKW"));
    }

    #[test]
    fn texto_sin_emotes() {
        let (plain, frags) = parse_content("solo texto");
        assert_eq!(plain, "solo texto");
        assert_eq!(frags.len(), 1);
        assert!(parse_content("").1.is_empty());
    }

    #[test]
    fn normaliza_mensaje_de_chat() {
        let body: Value = serde_json::from_str(CHAT_FIXTURE).unwrap();
        let ev = normalize("chat.message.sent", "01JBXYZ", &body).unwrap();

        assert_eq!(ev.platform, Platform::Kick as i32);
        assert_eq!(ev.platform_event_id, "01JBXYZ");
        assert_eq!(ev.channel.as_ref().unwrap().slug, "micanal");
        assert!(ev.occurred_at.is_some());

        let Some(Payload::Message(m)) = ev.payload else { panic!("no es un mensaje") };
        assert_eq!(m.text, "buenas KEKW");
        let sender = m.sender.unwrap();
        assert_eq!(sender.username, "espectador");
        assert!(sender.is_moderator);
        assert_eq!(sender.color, "#FF5733");
    }
}
