use std::collections::HashSet;
use std::time::Duration;

use actlet::prelude::*;
use anyhow::bail;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use super::*;
use crate::config::ProgramSourceConfig;

pub(super) async fn run<E>(source: ProgramSourceConfig, epg: E)
where
    E: Emit<UpdateProgramSections>,
{
    let mut previous = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), load(&source.command)).await {
            Ok(Ok(bytes)) if bytes != previous => match serde_json::from_slice(&bytes) {
                Ok(sections) => {
                    epg.emit(UpdateProgramSections {
                        services: source.services.clone(),
                        sections,
                    })
                    .await;
                    previous = bytes;
                }
                Err(err) => tracing::warn!(%err, "Invalid EPG source sections"),
            },
            Ok(Ok(_)) => (),
            Ok(Err(err)) => tracing::warn!(%err, "Failed to import EPG sections"),
            Err(_) => tracing::warn!("EPG section source timed out"),
        }
        tokio::time::sleep(source.interval).await;
    }
}

async fn load(command: &str) -> anyhow::Result<Vec<u8>> {
    let mut pipeline = crate::command_util::spawn_pipeline(
        vec![command.to_owned()],
        "program-source",
        "epg.program-source",
    )?;
    let (input, output) = pipeline.take_endpoints();
    drop(input);
    let mut bytes = Vec::new();
    output
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 16 * 1024 * 1024 {
        bail!("EPG source exceeds 16 MiB");
    }
    for status in pipeline.wait().await {
        if !status?.success() {
            bail!("EPG source command failed");
        }
    }
    Ok(bytes)
}

#[derive(Deserialize)]
struct ProgramSection {
    date: chrono::NaiveDate,
    section: EitSection,
}

#[derive(Message)]
pub(super) struct UpdateProgramSections {
    services: Vec<ServiceId>,
    sections: Vec<ProgramSection>,
}

#[async_trait]
impl<T> Handler<UpdateProgramSections> for Epg<T>
where
    T: Clone + Send + Sync + 'static,
    T: Call<StartStreaming>,
    T: TriggerFactory<StopStreaming>,
{
    async fn handle(&mut self, msg: UpdateProgramSections, _ctx: &mut Context<Self>) {
        let services: HashSet<_> = msg.services.into_iter().collect();
        if services.iter().any(|id| !self.services.contains_key(id)) {
            tracing::warn!("EPG source service has not been scanned");
            return;
        }
        if msg.sections.iter().any(|entry| {
            let section = &entry.section;
            !services.contains(&section.service_id())
                || !section.is_valid()
                || section.version_number > 31
                || section.section_number > section.segment_last_section_number
                || section.segment_last_section_number > section.last_section_number
                || section.section_number / 8 != section.segment_last_section_number / 8
        }) {
            tracing::warn!("EPG source returned an invalid section");
            return;
        }
        let today = Jst::today();
        let mut updated = HashSet::new();
        for entry in msg.sections {
            if entry.date != today {
                continue;
            }
            let mut section = entry.section;
            section.events.retain(|event| {
                event.start_time().is_some()
                    && event
                        .duration()
                        .is_some_and(|duration| duration > chrono::Duration::zero())
            });
            let service_id = section.service_id();
            if updated.insert(service_id) {
                self.prepare_schedule(service_id, today);
            }
            self.update_schedule(section);
        }
        if updated.is_empty() {
            return;
        }
        for service_id in updated {
            self.flush_schedule(service_id).await;
        }
        self.save_schedules();
    }
}
