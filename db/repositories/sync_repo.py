from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models.source_sync_state import SourceSyncState


def get_or_create_sync_state(session: Session, source_name: str) -> SourceSyncState:
    state = session.scalar(
        select(SourceSyncState).where(SourceSyncState.source_name == source_name)
    )
    if state is not None:
        return state

    state = SourceSyncState(
        source_name=source_name,
        last_status="never_run",
        last_message="Nunca sincronizado",
    )
    session.add(state)
    session.flush()
    return state