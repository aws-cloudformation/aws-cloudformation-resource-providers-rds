package software.amazon.rds.dbcluster.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.rds.dbcluster.EngineMode;
import software.amazon.rds.dbcluster.ResourceModel;

public class ResourceModelHelperTest {

    private ResourceModel model;

    @BeforeEach
    void setUp() {
        model = new ResourceModel();
    }

    @Test
    void isRestoreToPointInTime_withSourceDBClusterIdentifier_returnsTrue() {
        model.setSourceDBClusterIdentifier("source-cluster-id");
        Assertions.assertTrue(ResourceModelHelper.isRestoreToPointInTime(model));
    }

    @Test
    void isRestoreToPointInTime_withNullSourceDBClusterIdentifier_returnsFalse() {
        model.setSourceDBClusterIdentifier(null);
        Assertions.assertFalse(ResourceModelHelper.isRestoreToPointInTime(model));
    }

    @Test
    void isRestoreToPointInTime_withEmptySourceDBClusterIdentifier_returnsFalse() {
        model.setSourceDBClusterIdentifier("");
        Assertions.assertFalse(ResourceModelHelper.isRestoreToPointInTime(model));
    }

    @Test
    void isRestoreFromSnapshot_withSnapshotIdentifier_returnsTrue() {
        model.setSnapshotIdentifier("snapshot-id");
        Assertions.assertTrue(ResourceModelHelper.isRestoreFromSnapshot(model));
    }

    @Test
    void isRestoreFromSnapshot_withNullSnapshotIdentifier_returnsFalse() {
        model.setSnapshotIdentifier(null);
        Assertions.assertFalse(ResourceModelHelper.isRestoreFromSnapshot(model));
    }

    @Test
    void isRestoreFromSnapshot_withEmptySnapshotIdentifier_returnsFalse() {
        model.setSnapshotIdentifier("");
        Assertions.assertFalse(ResourceModelHelper.isRestoreFromSnapshot(model));
    }

    @Test
    void shouldUpdateAfterCreate_withSnapshotRestore_returnsTrue() {
        model.setSnapshotIdentifier("snapshot-id");
        Assertions.assertTrue(ResourceModelHelper.shouldUpdateAfterCreate(model));
    }

    @Test
    void shouldUpdateAfterCreate_withPointInTimeRestore_returnsTrue() {
        model.setSourceDBClusterIdentifier("source-cluster-id");
        Assertions.assertTrue(ResourceModelHelper.shouldUpdateAfterCreate(model));
    }

    @Test
    void shouldUpdateAfterCreate_withNoRestore_returnsFalse() {
        model.setSnapshotIdentifier(null);
        model.setSourceDBClusterIdentifier(null);
        Assertions.assertFalse(ResourceModelHelper.shouldUpdateAfterCreate(model));
    }

    @Test
    void shouldEnableHttpEndpointV2AfterCreate_withEnabledHttpAndNotServerless_returnsTrue() {
        model.setEnableHttpEndpoint(true);
        model.setEngineMode(EngineMode.Provisioned.toString());
        Assertions.assertTrue(ResourceModelHelper.shouldEnableHttpEndpointV2AfterCreate(model));
    }

    @Test
    void shouldEnableHttpEndpointV2AfterCreate_withDisabledHttp_returnsFalse() {
        model.setEnableHttpEndpoint(false);
        model.setEngineMode(EngineMode.Provisioned.toString());
        Assertions.assertFalse(ResourceModelHelper.shouldEnableHttpEndpointV2AfterCreate(model));
    }

    @Test
    void shouldEnableHttpEndpointV2AfterCreate_withNullHttpEndpoint_returnsFalse() {
        model.setEnableHttpEndpoint(null);
        model.setEngineMode(EngineMode.Provisioned.toString());
        Assertions.assertFalse(ResourceModelHelper.shouldEnableHttpEndpointV2AfterCreate(model));
    }

    @Test
    void shouldEnableHttpEndpointV2AfterCreate_withServerlessEngineMode_returnsFalse() {
        model.setEnableHttpEndpoint(true);
        model.setEngineMode(EngineMode.Serverless.toString());
        Assertions.assertFalse(ResourceModelHelper.shouldEnableHttpEndpointV2AfterCreate(model));
    }
}
