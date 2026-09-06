package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderState;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;

public final class SnapshotService {

    private final OrderSnapshotRepository repository;
    private final SnapshotPolicy policy;

    public SnapshotService(
        OrderSnapshotRepository repository,
        SnapshotPolicy policy
    ) {
        this.repository = Objects.requireNonNull(
            repository,
            "repository is required"
        );
        this.policy = Objects.requireNonNull(
            policy,
            "policy is required"
        );
    }

    public boolean createIfRequired(
        Connection connection,
        OrderState state
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            state,
            "state is required"
        );

        if (!policy.shouldCreateSnapshot(state)) {
            return false;
        }

        repository.save(
            connection,
            new OrderSnapshot(
                state.orderId(),
                state.version(),
                state,
                Instant.now()
            )
        );

        return true;
    }
}