package software.amazon.rds.integration;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.rds.model.IntegrationStatus;

import java.util.Set;

public class IntegrationStatusUtil {

    private final Set<IntegrationStatus> VALID_CREATING_STATES = ImmutableSet.of(
            IntegrationStatus.CREATING,
            IntegrationStatus.SYNCING,
            IntegrationStatus.MODIFYING,
            IntegrationStatus.NEEDS_ATTENTION,
            IntegrationStatus.ACTIVE
    );

    private final Set<IntegrationStatus> STABILIZED_STATES = ImmutableSet.of(
            IntegrationStatus.NEEDS_ATTENTION,
            IntegrationStatus.ACTIVE
    );


    public boolean isValidCreatingStatus(IntegrationStatus status) {
        return VALID_CREATING_STATES.contains(status);
    }

    public boolean isStabilizedState(IntegrationStatus status) {
        return STABILIZED_STATES.contains(status);
    }

}
