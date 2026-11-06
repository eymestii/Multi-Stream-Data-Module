//! Traducción de eventos de EventSub al modelo común.

use serde::Deserialize;
use serde_json::{from_value, Value};

use super::Metadata;
use crate::proto::v1::{
    chat_event::Payload, message_fragment::Kind, Badge, Channel, ChatEvent, ChatMessage, Cheermote,
    ChatCleared, Cheer, Emote, Follow, GiftSubscription, Mention, MessageDeleted, MessageFragment,
    Platform, Raid, ReplyInfo, RewardRedemption, StreamStatus, Subscription, User, UserBanned,
};
use crate::util::{rfc3339_to_timestamp, text_fragment};

pub(super) fn normalize(sub_type: &str, meta: &Metadata, event: &Value) -> Option<ChatEvent> {
    let channel = event_channel(sub_type, event)?;

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
        "channel.chat.clear" => Payload::ChatCleared(ChatCleared::default()),
        "channel.ban" => Payload::UserBanned(UserBanned {
            user: Some(event_user(event)?),
            reason: string(event, "reason"),
            expires_at: event["ends_at"].as_str().and_then(rfc3339_to_timestamp),
            ..Default::default()
        }),
        "channel.follow" => Payload::Follow(Follow {
            follower: Some(event_user(event)?),
        }),
        "channel.subscribe" => {
            #[derive(Deserialize)]
            struct Sub {
                tier: String,
            }
            let sub: Sub = from_value(event.clone()).ok()?;
            Payload::Subscription(Subscription {
                subscriber: Some(event_user(event)?),
                tier: normalize_tier(&sub.tier),
                ..Default::default()
            })
        }
        "channel.subscription.message" => {
            #[derive(Deserialize)]
            struct Resub {
                tier: String,
                cumulative_months: i32,
                streak_months: Option<i32>,
                message: ResubMessage,
            }
            #[derive(Deserialize)]
            struct ResubMessage {
                text: String,
            }
            let sub: Resub = from_value(event.clone()).ok()?;
            Payload::Subscription(Subscription {
                subscriber: Some(event_user(event)?),
                tier: normalize_tier(&sub.tier),
                cumulative_months: sub.cumulative_months,
                streak_months: sub.streak_months.unwrap_or_default(),
                is_renewal: true,
                message: sub.message.text,
            })
        }
        "channel.subscription.gift" => {
            #[derive(Deserialize)]
            struct Gift {
                #[serde(default)]
                is_anonymous: bool,
                total: i32,
                tier: String,
            }
            let gift: Gift = from_value(event.clone()).ok()?;
            Payload::GiftSubscription(GiftSubscription {
                gifter: (!gift.is_anonymous).then(|| event_user(event)).flatten(),
                is_anonymous: gift.is_anonymous,
                total: gift.total,
                tier: normalize_tier(&gift.tier),
                ..Default::default()
            })
        }
        "channel.raid" => Payload::Raid(Raid {
            from_broadcaster: Some(User {
                id: string(event, "from_broadcaster_user_id"),
                username: string(event, "from_broadcaster_user_login"),
                display_name: string(event, "from_broadcaster_user_name"),
                ..Default::default()
            }),
            viewers: event["viewers"].as_i64().unwrap_or_default() as i32,
        }),
        "channel.cheer" => {
            let is_anonymous = event["is_anonymous"].as_bool().unwrap_or(false);
            Payload::Cheer(Cheer {
                user: (!is_anonymous).then(|| event_user(event)).flatten(),
                is_anonymous,
                amount: event["bits"].as_i64().unwrap_or_default() as i32,
                message: string(event, "message"),
            })
        }
        "channel.channel_points_custom_reward_redemption.add" => {
            Payload::RewardRedemption(RewardRedemption {
                user: Some(event_user(event)?),
                reward_id: string(&event["reward"], "id"),
                reward_title: string(&event["reward"], "title"),
                cost: event["reward"]["cost"].as_i64().unwrap_or_default() as i32,
                user_input: string(event, "user_input"),
                status: string(event, "status").to_lowercase(),
            })
        }
        "channel.channel_points_automatic_reward_redemption.add" => {
            let reward = &event["reward"];
            let reward_type = string(reward, "type");
            let message = string(&event["message"], "text");
            let user_input = if message.is_empty() {
                string(event, "user_input")
            } else {
                message
            };
            Payload::RewardRedemption(RewardRedemption {
                user: Some(event_user(event)?),
                reward_id: reward_type.clone(),
                reward_title: automatic_reward_title(&reward_type),
                cost: reward["cost"].as_i64().unwrap_or_default() as i32,
                user_input,
                status: "fulfilled".into(),
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
        channel: Some(channel),
        occurred_at: rfc3339_to_timestamp(&meta.message_timestamp),
        payload: Some(payload),
        raw_json: event.to_string(),
        ..Default::default()
    })
}

fn event_channel(sub_type: &str, event: &Value) -> Option<Channel> {
    if sub_type == "channel.raid" {
        let id = string(event, "to_broadcaster_user_id");
        return (!id.is_empty()).then_some(Channel {
            platform: Platform::Twitch as i32,
            id,
            slug: string(event, "to_broadcaster_user_login"),
            display_name: string(event, "to_broadcaster_user_name"),
        });
    }
    from_value::<Broadcaster>(event.clone())
        .ok()
        .map(Broadcaster::into_channel)
}

fn string(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().to_string()
}

fn event_user(event: &Value) -> Option<User> {
    let id = string(event, "user_id");
    let username = string(event, "user_login");
    let display_name = string(event, "user_name");
    (!id.is_empty() || !username.is_empty() || !display_name.is_empty()).then_some(User {
        id,
        username,
        display_name,
        ..Default::default()
    })
}

fn normalize_tier(tier: &str) -> String {
    match tier {
        "1000" => "1",
        "2000" => "2",
        "3000" => "3",
        other => other,
    }
    .to_string()
}

fn automatic_reward_title(reward_type: &str) -> String {
    match reward_type {
        "single_message_bypass_sub_mode" => "Enviar un mensaje en modo solo suscriptores",
        "send_highlighted_message" => "Resaltar mi mensaje",
        "random_sub_emote_unlock" => "Desbloquear un emote de suscriptor",
        "chosen_sub_emote_unlock" => "Elegir un emote para desbloquear",
        "chosen_modified_sub_emote_unlock" => "Modificar un emote",
        "message_effect" => "Efecto de mensaje",
        "gigantify_an_emote" => "Emote gigante",
        "celebration" => "Celebración",
        other if !other.is_empty() => other,
        _ => "Recompensa automática",
    }
    .to_string()
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

    fn metadata() -> Metadata {
        Metadata {
            message_id: "event-1".into(),
            message_type: "notification".into(),
            message_timestamp: "2026-11-06T18:00:00Z".into(),
            subscription_type: None,
        }
    }

    fn channel_event(extra: Value) -> Value {
        let mut event = serde_json::json!({
            "broadcaster_user_id": "10",
            "broadcaster_user_login": "canal",
            "broadcaster_user_name": "Canal",
            "user_id": "20",
            "user_login": "viewer",
            "user_name": "Viewer"
        });
        event.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        event
    }

    #[test]
    fn normaliza_eventos_del_canal() {
        let meta = metadata();

        let follow = normalize("channel.follow", &meta, &channel_event(serde_json::json!({}))).unwrap();
        assert!(matches!(follow.payload, Some(Payload::Follow(_))));

        let sub = normalize(
            "channel.subscribe",
            &meta,
            &channel_event(serde_json::json!({"tier": "1000"})),
        )
        .unwrap();
        let Some(Payload::Subscription(sub)) = sub.payload else { panic!("no es suscripción") };
        assert_eq!(sub.tier, "1");

        let cheer = normalize(
            "channel.cheer",
            &meta,
            &channel_event(serde_json::json!({
                "is_anonymous": false,
                "bits": 100,
                "message": "Cheer100"
            })),
        )
        .unwrap();
        let Some(Payload::Cheer(cheer)) = cheer.payload else { panic!("no es cheer") };
        assert_eq!(cheer.amount, 100);

        let reward = normalize(
            "channel.channel_points_custom_reward_redemption.add",
            &meta,
            &channel_event(serde_json::json!({
                "reward": {"id": "r1", "title": "Hidratarse", "cost": 500},
                "user_input": "agua",
                "status": "unfulfilled"
            })),
        )
        .unwrap();
        assert!(matches!(reward.payload, Some(Payload::RewardRedemption(_))));

        let automatic = normalize(
            "channel.channel_points_automatic_reward_redemption.add",
            &meta,
            &channel_event(serde_json::json!({
                "reward": {"type": "send_highlighted_message", "cost": 100},
                "message": {"text": "mírame"},
                "redeemed_at": "2026-11-06T18:00:00Z"
            })),
        )
        .unwrap();
        let Some(Payload::RewardRedemption(reward)) = automatic.payload else {
            panic!("no es recompensa automática")
        };
        assert_eq!(reward.reward_title, "Resaltar mi mensaje");
        assert_eq!(reward.user_input, "mírame");
    }

    #[test]
    fn normaliza_raid_hacia_el_canal() {
        let raid = serde_json::json!({
            "from_broadcaster_user_id": "30",
            "from_broadcaster_user_login": "origen",
            "from_broadcaster_user_name": "Origen",
            "to_broadcaster_user_id": "10",
            "to_broadcaster_user_login": "canal",
            "to_broadcaster_user_name": "Canal",
            "viewers": 42
        });
        let event = normalize("channel.raid", &metadata(), &raid).unwrap();
        assert_eq!(event.channel.unwrap().id, "10");
        let Some(Payload::Raid(raid)) = event.payload else { panic!("no es raid") };
        assert_eq!(raid.viewers, 42);
    }
}
