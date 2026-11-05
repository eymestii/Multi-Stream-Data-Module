//! Núcleo: deduplica, asigna ids internos, guarda un buffer para replay
//! y reparte los eventos a todos los suscriptores.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio::sync::broadcast;
use ulid::{Generator, Ulid};

use crate::proto::v1::{Channel, ChatEvent};

pub type SharedEvent = Arc<ChatEvent>;

#[derive(Debug, PartialEq, Eq)]
pub enum ReplayError {
    /// El id no es un ULID válido.
    InvalidId,
    /// El id ya salió del buffer (o nunca existió).
    NotInBuffer,
}

pub struct Hub {
    inner: Mutex<Inner>,
    tx: broadcast::Sender<SharedEvent>,
    include_raw_json: bool,
}

struct Inner {
    buffer: VecDeque<SharedEvent>,
    buffer_cap: usize,
    dedup: Dedup,
    ulids: Generator,
    channels: HashMap<(i32, String), Channel>,
}

impl Hub {
    /// `buffer_cap`: eventos guardados para replay.
    /// `subscriber_queue`: cuántos eventos puede atrasarse un cliente antes de ser desconectado.
    pub fn new(buffer_cap: usize, subscriber_queue: usize, include_raw_json: bool) -> Arc<Self> {
        let (tx, _) = broadcast::channel(subscriber_queue.max(1));
        let buffer_cap = buffer_cap.max(1);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::with_capacity(buffer_cap),
                buffer_cap,
                dedup: Dedup::new(buffer_cap * 4),
                ulids: Generator::new(),
                channels: HashMap::new(),
            }),
            tx,
            include_raw_json,
        })
    }

    pub fn register_channel(&self, channel: Channel) {
        let mut inner = self.inner.lock().expect("hub lock envenenado");
        merge_channel(&mut inner.channels, channel);
    }

    /// Publica un evento normalizado. Devuelve `None` si era un duplicado.
    pub fn publish(&self, mut event: ChatEvent) -> Option<SharedEvent> {
        if !self.include_raw_json {
            event.raw_json.clear();
        }

        let mut inner = self.inner.lock().expect("hub lock envenenado");

        if !event.platform_event_id.is_empty() {
            let key = format!("{}:{}", event.platform, event.platform_event_id);
            if !inner.dedup.insert(key) {
                tracing::debug!(platform_event_id = %event.platform_event_id, "evento duplicado descartado");
                return None;
            }
        }

        // ULID monótono: ordenable aunque lleguen varios eventos en el mismo milisegundo.
        let id = inner.ulids.generate().unwrap_or_else(|_| Ulid::new());
        event.id = id.to_string();
        event.received_at = Some(SystemTime::now().into());

        if let Some(channel) = &event.channel {
            merge_channel(&mut inner.channels, channel.clone());
        }

        let event = Arc::new(event);
        if inner.buffer.len() == inner.buffer_cap {
            inner.buffer.pop_front();
        }
        inner.buffer.push_back(event.clone());

        // Se envía con el lock tomado para que el orden del buffer y del broadcast sea el mismo.
        // Solo falla si no hay suscriptores, lo cual está bien.
        let _ = self.tx.send(event.clone());
        Some(event)
    }

    /// Se suscribe a eventos en vivo. Si `resume_after` viene, devuelve además los eventos
    /// del buffer posteriores a ese id. Ambas cosas se hacen bajo el mismo lock para que
    /// no haya huecos ni duplicados entre el backlog y lo que llega en vivo.
    pub fn subscribe(
        &self,
        resume_after: Option<&str>,
    ) -> Result<(Vec<SharedEvent>, broadcast::Receiver<SharedEvent>), ReplayError> {
        let inner = self.inner.lock().expect("hub lock envenenado");
        let backlog = match resume_after {
            None => Vec::new(),
            Some(raw) => {
                let wanted = Ulid::from_string(raw)
                    .map_err(|_| ReplayError::InvalidId)?
                    .to_string();
                let pos = inner
                    .buffer
                    .iter()
                    .position(|e| e.id == wanted)
                    .ok_or(ReplayError::NotInBuffer)?;
                inner.buffer.iter().skip(pos + 1).cloned().collect()
            }
        };
        Ok((backlog, self.tx.subscribe()))
    }

    pub fn channels(&self) -> Vec<Channel> {
        let inner = self.inner.lock().expect("hub lock envenenado");
        let mut channels: Vec<Channel> = inner.channels.values().cloned().collect();
        channels.sort_by(|a, b| (a.platform, &a.slug, &a.id).cmp(&(b.platform, &b.slug, &b.id)));
        channels
    }
}

fn merge_channel(map: &mut HashMap<(i32, String), Channel>, channel: Channel) {
    let entry = map
        .entry((channel.platform, channel.id.clone()))
        .or_insert_with(|| channel.clone());
    if !channel.slug.is_empty() {
        entry.slug = channel.slug;
    }
    if !channel.display_name.is_empty() {
        entry.display_name = channel.display_name;
    }
}

/// Conjunto acotado de ids vistos: cuando se llena, olvida los más antiguos.
struct Dedup {
    seen: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl Dedup {
    fn new(cap: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(cap),
            order: VecDeque::with_capacity(cap),
            cap: cap.max(1),
        }
    }

    /// Devuelve `true` si el id es nuevo.
    fn insert(&mut self, key: String) -> bool {
        if self.seen.contains(&key) {
            return false;
        }
        if self.order.len() == self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.seen.insert(key);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::Platform;

    fn event(platform_event_id: &str) -> ChatEvent {
        ChatEvent {
            platform: Platform::Twitch as i32,
            platform_event_id: platform_event_id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn descarta_duplicados() {
        let hub = Hub::new(10, 10, false);
        assert!(hub.publish(event("a")).is_some());
        assert!(hub.publish(event("a")).is_none());
        assert!(hub.publish(event("b")).is_some());
    }

    #[test]
    fn ids_internos_son_crecientes() {
        let hub = Hub::new(10, 10, false);
        let a = hub.publish(event("a")).unwrap();
        let b = hub.publish(event("b")).unwrap();
        assert!(a.id < b.id);
    }

    #[test]
    fn replay_devuelve_lo_posterior() {
        let hub = Hub::new(10, 10, false);
        let a = hub.publish(event("a")).unwrap();
        hub.publish(event("b"));
        hub.publish(event("c"));
        let (backlog, _rx) = hub.subscribe(Some(&a.id)).unwrap();
        let ids: Vec<_> = backlog.iter().map(|e| e.platform_event_id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);
    }

    #[test]
    fn replay_falla_si_el_evento_salio_del_buffer() {
        let hub = Hub::new(2, 10, false);
        let a = hub.publish(event("a")).unwrap();
        hub.publish(event("b"));
        hub.publish(event("c"));
        assert_eq!(hub.subscribe(Some(&a.id)).unwrap_err(), ReplayError::NotInBuffer);
        assert_eq!(hub.subscribe(Some("no-es-ulid")).unwrap_err(), ReplayError::InvalidId);
    }

    #[test]
    fn raw_json_se_limpia_si_no_esta_habilitado() {
        let hub = Hub::new(10, 10, false);
        let mut ev = event("a");
        ev.raw_json = "{}".into();
        assert!(hub.publish(ev).unwrap().raw_json.is_empty());
    }
}
