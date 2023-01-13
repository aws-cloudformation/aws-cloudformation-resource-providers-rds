package software.amazon.rds.dbinstance.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.rds.dbinstance.ResourceModel;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
                .allocatedStorage("100")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
    }

    @Test
    public void shouldUpdateAfterCreate_whenReadReplicaAndAllocatedStorage() {
        final ResourceModel model = ResourceModel.builder()
                .sourceDBInstanceIdentifier("identifier")
                .allocatedStorage("100")
                .build();

        assertThat(ResourceModelHelper.shouldUpdateAfterCreate(model)).isFalse();
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
}
