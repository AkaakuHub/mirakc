use std::sync::Arc;
use std::time::Duration;

use actlet::prelude::*;
use anyhow::bail;
use indexmap::IndexMap;
use tokio::io::AsyncReadExt;

use super::*;
use crate::config::ProgramSourceConfig;

pub(super) async fn run<E>(source: ProgramSourceConfig, epg: E)
where
    E: Emit<ReplacePrograms>,
{
    loop {
        match tokio::time::timeout(Duration::from_secs(10), load(&source.command)).await {
            Ok(Ok(programs)) => {
                epg.emit(ReplacePrograms {
                    services: source.services.clone(),
                    programs,
                })
                .await;
            }
            Ok(Err(err)) => tracing::warn!(%err, "Failed to import EPG programs"),
            Err(_) => tracing::warn!("EPG program source timed out"),
        }
        tokio::time::sleep(source.interval).await;
    }
}

async fn load(command: &str) -> anyhow::Result<Vec<EpgProgram>> {
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
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Message)]
pub(super) struct ReplacePrograms {
    services: Vec<ServiceId>,
    programs: Vec<EpgProgram>,
}

#[async_trait]
impl<T> Handler<ReplacePrograms> for Epg<T>
where
    T: Clone + Send + Sync + 'static,
    T: Call<StartStreaming>,
    T: TriggerFactory<StopStreaming>,
{
    async fn handle(&mut self, msg: ReplacePrograms, _ctx: &mut Context<Self>) {
        let mut grouped: HashMap<ServiceId, IndexMap<Eid, EpgProgram>> = msg
            .services
            .into_iter()
            .map(|id| (id, IndexMap::new()))
            .collect();
        if grouped.keys().any(|id| !self.services.contains_key(id)) {
            tracing::warn!("EPG source service has not been scanned");
            return;
        }
        for program in msg.programs {
            let service_id = ServiceId::from(program.id);
            let Some(programs) = grouped.get_mut(&service_id) else {
                tracing::warn!(%program.id, "EPG source returned an unconfigured service");
                return;
            };
            if program.start_at.is_none()
                || program
                    .duration
                    .is_none_or(|duration| duration <= chrono::Duration::zero())
                || programs.contains_key(&program.id.eid())
            {
                tracing::warn!(%program.id, "EPG source returned an invalid program");
                return;
            }
            programs.insert(program.id.eid(), program);
        }
        for (service_id, programs) in grouped {
            self.prepare_schedule(service_id, chrono_jst::Jst::today());
            self.schedules
                .get_mut(&service_id)
                .unwrap()
                .imported_programs = Some(Arc::new(programs));
            self.flush_schedule(service_id).await;
        }
        self.save_schedules();
    }
}
