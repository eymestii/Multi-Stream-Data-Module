//! Capa de ingesta: un adaptador por plataforma. Cada uno traduce los eventos nativos
//! al modelo común (`ChatEvent`) y los publica en el hub.

pub mod kick;
pub mod mock;
pub mod twitch;
