package software.amazon.rds.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.rds.model.IntegrationStatus;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IntegrationStatusUtilTest {

    @Test
    public void assertKnownStatuses() {
        // if this test fails, this means there is new status that this handler does not know about.
        // In that case, check the new status, make any changes necessary to fix the handler, then
        // add the state to the list of known states.
        Set<IntegrationStatus> allStatuses = new HashSet<>(Arrays.asList(IntegrationStatus.values()));
        allStatuses.remove(IntegrationStatus.UNKNOWN_TO_SDK_VERSION);
        allStatuses.remove(IntegrationStatus.CREATING);
        allStatuses.remove(IntegrationStatus.ACTIVE);
        allStatuses.remove(IntegrationStatus.MODIFYING);
        allStatuses.remove(IntegrationStatus.FAILED);
        allStatuses.remove(IntegrationStatus.DELETING);
        allStatuses.remove(IntegrationStatus.SYNCING);
        allStatuses.remove(IntegrationStatus.NEEDS_ATTENTION);
        Assertions.assertTrue(allStatuses.isEmpty(),
                "There are new integration statuses that this handler does not know about: " +
                allStatuses +
                "Please check the handler code, and then add them to the list of known statuses");
    }
}
