package software.amazon.rds.dbinstance.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

import software.amazon.rds.dbinstance.ResourceModel;

public class ResourceModelHelperTest {

    @Test
    public void isReadReplica_whenSourceIdentifierIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("identifier")
                .build();

        assertThat(ResourceModelHelper.isReadReplica(model)).isTrue();
    }

    @Test
    public void isReadReplica_whenSourceIdentifierIsEmpty() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("")
                .build();

        assertThat(ResourceModelHelper.isReadReplica(model)).isFalse();
    }

    @Test
    public void isRestoreFromSnapshot_whenSnapshotIdentifierIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .build();

        assertThat(ResourceModelHelper.isRestoreFromSnapshot(model)).isTrue();
    }

    @Test
    public void isRestoreFromClusterSnapshot_whenClusterSnapshotIdentifierIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .dBClusterSnapshotIdentifier("identifier")
                .build();

        assertThat(ResourceModelHelper.isRestoreFromClusterSnapshot(model)).isTrue();
    }

    @Test
    public void isRestoreFromClusterSnapshot_whenClusterSnapshotIdentifierIsEmpty() {
        final ResourceModel model = ResourceModel.builder()
                .dBClusterSnapshotIdentifier("")
                .build();

        assertThat(ResourceModelHelper.isRestoreFromClusterSnapshot(model)).isFalse();
    }

    @Test
    public void isRestoreFromSnapshot_whenSnapshotIdentifierIsEmpty() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("")
                .build();

        assertThat(ResourceModelHelper.isRestoreFromSnapshot(model)).isFalse();
    }

    @Test
    public void shouldUpdateAfterCreate_whenModelIsEmpty() {
        final ResourceModel model = ResourceModel.builder()
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndAllocatedStorage() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .engine("postgresql")
                .allocatedStorage("100")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndMasterUserPassword() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .masterUserPassword("password")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenReadReplicaAndPreferredMaintenanceWindow() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("identifier")
                .preferredMaintenanceWindow("window")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenReadReplicaAndMaxAllocatedStorage() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("identifier")
                .maxAllocatedStorage(100)
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenCertificateAuthorityAppliedAndAllocatedStorage() {
        final ResourceModel model = ResourceModel.builder()
                .cACertificateIdentifier("identifier")
                .allocatedStorage("100")
                .dBSnapshotIdentifier("identifier")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenStorageThroughputIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .storageThroughput(100)
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenSqlServerAndAllocatedStorage() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .allocatedStorage("100")
                .engine("sqlserver")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndManageMasterUserPassword() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .manageMasterUserPassword(true)
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenDeletionProtectionIsTrue() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("source-db-instance-identifier")
                .deletionProtection(true)
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_enablePerformanceInsights() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("db-snapshot-identifier")
                .enablePerformanceInsights(true)
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldNotUpdateAfterCreate_enablePerformanceInsightsIsFalse() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("db-snapshot-identifier")
                .enablePerformanceInsights(false)
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }

    @Test
    public void shouldNotUpdateAfterCreate_enablePerformanceInsightsIsNull() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("db-snapshot-identifier")
                .enablePerformanceInsights(null)
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }

    @Test
    public void shouldUpdateAfterCreate_monitoringIntervalIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("db-snapshot-identifier")
                .monitoringInterval(42)
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_monitoringRoleArnIsSet() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("db-snapshot-identifier")
                .monitoringRoleArn("monitoring-role-arn")
                .build();
        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndStorageTypeSpecified() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndSqlServerEngineAndStorageTypeSpecified() {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("snapshot")
                .engine("sqlserver")
                .storageType("gp2")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSnapshotAndAuroraEngineAndStorageTypeSpecified() {
        final ResourceModel model = ResourceModel.builder()
                .engine("aurora-postgres")
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }
}
