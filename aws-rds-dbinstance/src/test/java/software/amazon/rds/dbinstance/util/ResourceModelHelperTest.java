package software.amazon.rds.dbinstance.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    @ParameterizedTest
    @ValueSource(strings = {
            "aurora-mysql",
            "aurora-postgresql",
            "custom-oracle-ee",
            "mariadb",
            "mysql",
            "oracle-ee",
            "oracle-ee-cdb",
            "oracle-se2",
            "oracle-se2-cdb",
            "postgres"})
    public void shouldUpdateAfterCreate_whenRestoreFromNonSqlServerSnapshotAndAllocatedStorage(final String engine) {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .engine(engine)
                .allocatedStorage("100")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "sqlserver-ee",
            "sqlserver-se",
            "sqlserver-ex",
            "sqlserver-web"})
    public void shouldUpdateAfterCreate_whenRestoreFromSqlServerSnapshotAndAllocatedStorage(final String engine) {
        final ResourceModel model = ResourceModel.builder()
                .dBSnapshotIdentifier("identifier")
                .engine(engine)
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
    public void shouldUpdateAfterCreate_whenRestoreFromAuroraPostgresSnapshotAndAuroraEngineAndStorageTypeSpecified() {
        final ResourceModel model = ResourceModel.builder()
                .engine("aurora-postgres")
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }

    @Test
    public void shouldUpdateAfterCreate_whenRestoreFromSqlServerSnapshotAndAuroraEngineAndStorageTypeSpecified() {
        final ResourceModel model = ResourceModel.builder()
                .engine("sqlserver-ee")
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isTrue();
    }

    @Test
    public void getBackupRetentionPeriod_returnsZeroWhenNotSet() {
        final ResourceModel model = ResourceModel.builder()
                .build();

        assertThat(ResourceModelHelper.getBackupRetentionPeriod(model)).isEqualTo(0);
    }

    @Test
    public void getBackupRetentionPeriod_returnsValueWhenSet() {
        final ResourceModel model = ResourceModel.builder()
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.getBackupRetentionPeriod(model)).isEqualTo(10);
    }

    @Test
    public void getAutomaticBackupReplicationRegion_returnsNullWhenBackupReplicationIsZero() {
        final ResourceModel model = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .build();

        assertThat(ResourceModelHelper.getAutomaticBackupReplicationRegion(model)).isNull();
    }

    @Test
    public void getAutomaticBackupReplicationRegion_returnsValueWhenBackupReplicationIsZero() {
        final ResourceModel model = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.getAutomaticBackupReplicationRegion(model)).isEqualTo("eu-west-1");
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsFalseWhenRegionUnchanged() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(previous, desired)).isFalse();
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsTrueWhenRegionChanged() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-2")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(previous, desired)).isTrue();
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsTrueWhenBackupRetentionPeriodChangedFromNullToValue() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(previous, desired)).isTrue();
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsFalseWhenBackupRetentionPeriodChangedFromOneToTwo() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(1)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(2)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(previous, desired)).isFalse();
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsTrueWhePreviousModelIsNull() {
        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(2)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(null, desired)).isTrue();
    }

    @Test
    public void shouldStartAutomaticBackupReplication_returnsFalseWhePreviousModelIsNullAndFeatureNotEnabled() {
        final ResourceModel desired = ResourceModel.builder()
                .backupRetentionPeriod(2)
                .build();

        assertThat(ResourceModelHelper.shouldStartAutomaticBackupReplication(null, desired)).isFalse();
    }

    @Test
    public void shouldStopAutomaticBackupReplication_returnsFalseWhenPreviousRegionIsNull() {
        final ResourceModel previous = ResourceModel.builder()
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isFalse();
    }


    @Test
    public void shouldStopAutomaticBackupReplication_returnsFalseWhenRegionUnchanged() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isFalse();
    }

    @Test
    public void shouldStopAutomaticBackupReplication_returnsTrueWhenRegionChanged() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-2")
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isTrue();
    }

    @Test
    public void shouldStopAutomaticBackupReplication_returnsTrueWhenBackupRetentionPeriodChangedFromValueToNull() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(10)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isTrue();
    }

    @Test
    public void shouldStopAutomaticBackupReplication_returnsFalseWhenBackupRetentionPeriodChangedFromOneToTwo() {
        final ResourceModel previous = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(1)
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .automaticBackupReplicationRegion("eu-west-1")
                .backupRetentionPeriod(2)
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isFalse();
    }

    @Test
    public void shouldStopAutomaticBackupReplication_returnsTrueWhenRegionSetFromValuToNull() {
        final ResourceModel previous = ResourceModel.builder()
                .backupRetentionPeriod(10)
                .automaticBackupReplicationRegion("eu-west-1")
                .build();

        final ResourceModel desired = ResourceModel.builder()
                .backupRetentionPeriod(10)
                .build();

        assertThat(ResourceModelHelper.shouldStopAutomaticBackupReplication(previous, desired)).isTrue();
    }
}
