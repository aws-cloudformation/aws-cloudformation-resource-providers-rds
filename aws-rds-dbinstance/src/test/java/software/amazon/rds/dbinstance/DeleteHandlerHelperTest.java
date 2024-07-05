package software.amazon.rds.dbinstance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.DB_INSTANCE_ACTIVE;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.DB_INSTANCE_AURORA_ACTIVE;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.DB_INSTANCE_MAZ_CLUSTER_READ_REPLICA_ACTIVE;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.DB_INSTANCE_READ_REPLICA_ACTIVE;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.RESOURCE_MODEL_BLDR;

class DeleteHandlerHelperTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void decideSnapshotIdentifier_FollowsSnapshotRequested(boolean isSnapshotRequested) {
        ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier("dbinstance")
                .clientRequestToken("token")
                .snapshotRequested(isSnapshotRequested)
                .build();

        final String snapshotIdentifier = DeleteHandler.decideSnapshotIdentifier(request, DB_INSTANCE_ACTIVE);

        if (isSnapshotRequested) {
            assertThat(snapshotIdentifier).isNotBlank();
        } else {
            assertThat(snapshotIdentifier).isNull();
        }
    }

    @Test
    void decideSnapshotIdentifier_DefaultsToSnapshot() {
        ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier("dbinstance")
                .clientRequestToken("token")
                .build();

        final String snapshotIdentifier = DeleteHandler.decideSnapshotIdentifier(request, DB_INSTANCE_ACTIVE);

        assertThat(snapshotIdentifier).isNotBlank();
    }

    void testDecideSnapshotIdentifierImpossible(DBInstance instance) {
        ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier("dbinstance")
                .clientRequestToken("token")
                .snapshotRequested(true)
                .build();

        final String snapshotIdentifier = DeleteHandler.decideSnapshotIdentifier(request, instance);

        assertThat(snapshotIdentifier).isNull();
    }

    @Test
    void decideSnapshotIdentifier_NoSnapshotClusterInstance() {
        testDecideSnapshotIdentifierImpossible(DB_INSTANCE_AURORA_ACTIVE);
    }

    @Test
    void decideSnapshotIdentifier_NoSnapshotReadReplica() {
        testDecideSnapshotIdentifierImpossible(DB_INSTANCE_READ_REPLICA_ACTIVE);
    }

    @Test
    void decideSnapshotIdentifier_NoSnapshotReadReplicaOfMultiAzCluster() {
        testDecideSnapshotIdentifierImpossible(DB_INSTANCE_MAZ_CLUSTER_READ_REPLICA_ACTIVE);
    }

}
