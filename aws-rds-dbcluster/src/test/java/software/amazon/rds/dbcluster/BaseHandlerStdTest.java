package software.amazon.rds.dbcluster;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ClusterPendingModifiedValues;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

class BaseHandlerStdTest {

    class TestBaseHandlerStd extends BaseHandlerStd {

        public TestBaseHandlerStd(HandlerConfig config) {
            super(config);
        }

        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy, ResourceHandlerRequest<ResourceModel> request, CallbackContext callbackContext, ProxyClient<RdsClient> rdsProxyClient, ProxyClient<Ec2Client> ec2ProxyClient, Logger logger) {
            return null;
        }

        @Override
        public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
                AmazonWebServicesClientProxy proxy,
                ResourceHandlerRequest<ResourceModel> request,
                CallbackContext callbackContext,
                Logger logger
        ) {
            return null;
        }
    }

    private TestBaseHandlerStd handler;

    @BeforeEach
    public void setUp() {
        handler = new TestBaseHandlerStd(null);
    }

    @Test
    void isNoPendingChanges_NonEmptyDBClusterIdentifier() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .dbClusterIdentifier("DBClusterIdentifier")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyMasterUserPassword() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .masterUserPassword("MasterUserPassword")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyIAMDatabaseAuthenticationEnabled() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .iamDatabaseAuthenticationEnabled(true)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyEngineVersion() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .engineVersion("11.3")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyBackupRetentionPeriod() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .backupRetentionPeriod(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyAllocatedStorage() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .allocatedStorage(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyIops() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBCluster.builder()
                        .pendingModifiedValues(
                                ClusterPendingModifiedValues.builder()
                                        .iops(42)
                                        .build())
                        .build()
        )).isFalse();
    }
}
