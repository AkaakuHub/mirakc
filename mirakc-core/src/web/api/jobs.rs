use super::*;

#[derive(serde::Serialize, utoipa::ToSchema)]
pub(super) struct ScheduleCollection {
    status: epg::ScheduleCollectionStatus,
}

/// Collects TV program schedules using the existing EPG job.
#[utoipa::path(
    post,
    path = "/jobs/update-schedules",
    responses(
        (status = 202, description = "Collection started", body = ScheduleCollection),
        (status = 200, description = "Collection already running", body = ScheduleCollection),
        (status = 500, description = "Internal Server Error"),
    ),
    operation_id = "updateSchedules",
)]
pub(super) async fn update_schedules<E>(
    State(EpgExtractor(epg)): State<EpgExtractor<E>>,
) -> Result<(StatusCode, Json<ScheduleCollection>), Error>
where
    E: Call<epg::InvokeUpdateSchedules>,
{
    let status = epg.call(epg::InvokeUpdateSchedules).await??;
    let code = match status {
        epg::ScheduleCollectionStatus::Started => StatusCode::ACCEPTED,
        epg::ScheduleCollectionStatus::Running => StatusCode::OK,
    };
    Ok((code, Json(ScheduleCollection { status })))
}
