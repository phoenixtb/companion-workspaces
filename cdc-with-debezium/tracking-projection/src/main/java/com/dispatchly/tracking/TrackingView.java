package com.dispatchly.tracking;

import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

// The read model. Every write is idempotent: applying the same change twice, or an older change after
// a newer one, leaves the row as it was.
@Repository
class TrackingView {

    private final JdbcClient jdbc;

    TrackingView(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // Applies c, u and r events. Returns false when the view already holds this version or a newer one,
    // or when the shipment has been deleted.
    boolean upsert(ShipmentChange.Row row, ShipmentChange.Source source) {
        return jdbc.sql("""
                INSERT INTO tracking_view AS v
                    (shipment_id, tracking_code, status, hub_id, eta, version, source_lsn, changed_at)
                VALUES (:id, :code, :status, :hub, :eta, :version, :lsn, :changedAt)
                ON CONFLICT (shipment_id) DO UPDATE
                SET tracking_code = EXCLUDED.tracking_code, status = EXCLUDED.status, hub_id = EXCLUDED.hub_id,
                    eta = EXCLUDED.eta, version = EXCLUDED.version, source_lsn = EXCLUDED.source_lsn,
                    changed_at = EXCLUDED.changed_at
                WHERE v.version < EXCLUDED.version AND NOT v.deleted""")
                .param("id", row.id())
                .param("code", row.trackingCode())
                .param("status", row.status())
                .param("hub", row.hubId())
                .param("eta", row.eta() == null ? null : Timestamp.from(row.eta()))
                .param("version", row.version())
                .param("lsn", source.lsn())
                .param("changedAt", Timestamp.from(Instant.ofEpochMilli(source.tsMs())))
                .update() == 1;
    }

    // Applies d events. A delete is terminal: the row stays as a marker, so a replayed older change
    // for the same id can't bring the shipment back.
    boolean markDeleted(long shipmentId, ShipmentChange.Source source) {
        return jdbc.sql("""
                INSERT INTO tracking_view AS v (shipment_id, deleted, source_lsn, changed_at)
                VALUES (:id, true, :lsn, :changedAt)
                ON CONFLICT (shipment_id) DO UPDATE
                SET deleted = true, source_lsn = EXCLUDED.source_lsn, changed_at = EXCLUDED.changed_at
                WHERE NOT v.deleted""")
                .param("id", shipmentId)
                .param("lsn", source.lsn())
                .param("changedAt", Timestamp.from(Instant.ofEpochMilli(source.tsMs())))
                .update() == 1;
    }
}
