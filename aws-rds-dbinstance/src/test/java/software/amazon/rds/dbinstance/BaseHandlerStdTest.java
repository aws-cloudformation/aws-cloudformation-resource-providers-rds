package software.amazon.rds.dbinstance;

import java.time.Instant;
import java.util.Collections;

import junit.framework.Assert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceStatusInfo;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.PendingCloudwatchLogsExports;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.awssdk.services.rds.model.ProcessorFeature;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

class BaseHandlerStdTest {

    class TestBaseHandlerStd extends BaseHandlerStd {

        public TestBaseHandlerStd(HandlerConfig config) {
            super(config);
        }

        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(AmazonWebServicesClientProxy proxy, ResourceHandlerRequest<ResourceModel> request, CallbackContext context, VersionedProxyClient<RdsClient> rdsProxyClient, VersionedProxyClient<Ec2Client> ec2ProxyClient, Logger logger) {
            return null;
        }
    }

    private TestBaseHandlerStd handler;

    @BeforeEach
    public void setUp() {
        handler = new TestBaseHandlerStd(null);
    }

    @Test
    void isPromotionTierUpdated_NullPromotionTierReturnsTrue() {
        Assert.assertTrue(handler.isPromotionTierUpdated(
                DBInstance.builder().build(),
                ResourceModel.builder().build()
        ));
    }

    @Test
    void isPromotionTierUpdated_MatchingPromotionTiedReturnsTrue() {
        Assert.assertTrue(handler.isPromotionTierUpdated(
                DBInstance.builder().promotionTier(42).build(),
                ResourceModel.builder().promotionTier(42).build()
        ));
    }

    @Test
    void isPromotionTierUpdated_MismatchingPromotionTierReturnsFalse() {
        Assert.assertFalse(handler.isPromotionTierUpdated(
                DBInstance.builder().promotionTier(42).build(),
                ResourceModel.builder().promotionTier(null).build()
        ));
    }

    @Test
    void isDomainMembershipsJoined_NullDomainMembershipReturnsTrue() {
        Assert.assertTrue(handler.isDomainMembershipsJoined(
                DBInstance.builder().build()
        ));
    }

    @Test
    void isDomainMembershipsJoined_EmptyDomainMembershipReturnsTrue() {
        Assert.assertTrue(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(Collections.emptyList())
                        .build()
        ));
    }

    @Test
    void isDomainMembershipsJoined_NonEmptyListJoinedAndKerberosReturnsTrue() {
        Assert.assertTrue(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(
                                DomainMembership.builder().status("joined").build(),
                                DomainMembership.builder().status("kerberos-enabled").build()
                        )
                        .build()
        ));
    }

    @Test
    void isDomainMembershipsJoined_NonEmptyListJoinedAndKerberosAndAnythingElseReturnsFalse() {
        Assert.assertFalse(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(
                                DomainMembership.builder().status("joined").build(),
                                DomainMembership.builder().status("kerberos-enabled").build(),
                                DomainMembership.builder().status("something else").build()
                        )
                        .build()
        ));
    }

    @Test
    void isVpcSecurityGroupsActive_NullVpcSecurityGroupsReturnsTrue() {
        Assert.assertTrue(handler.isVpcSecurityGroupsActive(
                DBInstance.builder().build()
        ));
    }

    @Test
    void isVpcSecurityGroupsActive_EmptyVpcSecurityGroupsReturnsTrue() {
        Assert.assertTrue(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(Collections.emptyList())
                        .build()
        ));
    }

    @Test
    void isVpcSecurityGroupsActive_NonEmptyVpcSecurityGroupsActiveReturnsTrue() {
        Assert.assertTrue(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(
                                VpcSecurityGroupMembership.builder().status("active").build(),
                                VpcSecurityGroupMembership.builder().status("active").build()
                        )
                        .build()
        ));
    }

    @Test
    void isVpcSecurityGroupsActive_NonEmptyVpcSecurityGroupsNotActiveReturnsFalse() {
        Assert.assertFalse(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(
                                VpcSecurityGroupMembership.builder().status("active").build(),
                                VpcSecurityGroupMembership.builder().status(null).build()
                        )
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NullPendingChangesReturnsTrue() {
        Assert.assertTrue(handler.isNoPendingChanges(
                DBInstance.builder().build()
        ));
    }

    @Test
    void isNoPendingChanges_EmptyPendingChangesReturnsTrue() {
        Assert.assertTrue(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(PendingModifiedValues.builder().build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyAllocatedStorageReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .allocatedStorage(42)
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyCACertificateIdentifierReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .caCertificateIdentifier("certificate")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyMasterUserPasswordReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .masterUserPassword("password")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyBackupRetentionPeriodReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .backupRetentionPeriod(42)
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyMultiAZReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .multiAZ(true)
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyEngineVersionReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .engineVersion("1.2.3")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyIopsReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .iops(42)
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyDBInstanceIdentifierReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .dbInstanceIdentifier("dbinstance")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyLicenseModelReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .licenseModel("license model")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyStorageTypeReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .storageType("storage type")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyDBSubnetGroupNameReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .dbSubnetGroupName("db-subnet-group-name")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyPendingCloudWatchLogsExportsReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .pendingCloudwatchLogsExports(PendingCloudwatchLogsExports.builder().build())
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_EmptyProcessorFeaturesReturnsTrue() {
        Assert.assertTrue(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .processorFeatures(Collections.emptyList())
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyIamDatabaseAutenticationEnabledReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .iamDatabaseAuthenticationEnabled(true)
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyAutomationModeReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .automationMode("automation mode")
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyResumeFullAutomationModeTimeReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .resumeFullAutomationModeTime(Instant.now())
                                        .build())
                        .build()
        ));
    }

    @Test
    void isNoPendingChanges_NonEmptyReturnsFalse() {
        Assert.assertFalse(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .processorFeatures(ProcessorFeature.builder().build())
                                        .build())
                        .build()
        ));
    }

    @Test
    void isDBParameterGroupSyncComplete_NullParameterGroupsReturnsTrue() {
        Assert.assertTrue(handler.isDBParameterGroupSyncComplete(
                DBInstance.builder().build()
        ));
    }

    @Test
    void isDBParameterGroupSyncComplete_EmptyParameterGroupsReturnsTrue() {
        Assert.assertTrue(handler.isDBParameterGroupSyncComplete(
                DBInstance.builder().dbParameterGroups(Collections.emptyList()).build()
        ));
    }

    @Test
    void isDBParameterGroupSyncComplete_NonEmptyParameterGroupsInSyncReturnsTrue() {
        Assert.assertTrue(handler.isDBParameterGroupSyncComplete(
                DBInstance.builder()
                        .dbParameterGroups(
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build(),
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build()
                        )
                        .build()
        ));
    }

    @Test
    void isDBParameterGroupSyncComplete_NonEmptyParameterGroupsApplyingReturnsFalse() {
        Assert.assertFalse(handler.isDBParameterGroupSyncComplete(
                DBInstance.builder()
                        .dbParameterGroups(
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build(),
                                DBParameterGroupStatus.builder().parameterApplyStatus("applying").build()
                        )
                        .build()
        ));
    }

    @Test
    void isReplicationComplete_NullStatusInfoReturnsTrue() {
        Assert.assertTrue(handler.isReplicationComplete(
                DBInstance.builder().build()
        ));
    }

    @Test
    void isReplicationComplete_EmptyStatusInfoReturnsTrue() {
        Assert.assertTrue(handler.isReplicationComplete(
                DBInstance.builder().statusInfos(Collections.emptyList()).build()
        ));
    }

    @Test
    void isReplicationComplete_NonEmptyListNoReadReplicaInfoReturnsTrue() {
        Assert.assertTrue(handler.isReplicationComplete(
                DBInstance.builder()
                        .statusInfos(DBInstanceStatusInfo.builder()
                                .statusType("something else")
                                .status("not replicating")
                                .build())
                        .build()
        ));
    }

    @Test
    void isReplicationComplete_NonEmptyListContainingReplicaInfoReplicatingReturnsTrue() {
        Assert.assertTrue(handler.isReplicationComplete(
                DBInstance.builder()
                        .statusInfos(
                                DBInstanceStatusInfo.builder()
                                        .statusType("read replication")
                                        .status("replicating")
                                        .build(),
                                DBInstanceStatusInfo.builder()
                                        .statusType("something else")
                                        .status("not replicating")
                                        .build()
                        )
                        .build()
        ));
    }

    @Test
    void isReplicationComplete_NonEmptyListContainingReplicaInfoNotReplicatingReturnsFalse() {
        Assert.assertFalse(handler.isReplicationComplete(
                DBInstance.builder()
                        .statusInfos(
                                DBInstanceStatusInfo.builder()
                                        .statusType("read replication")
                                        .status("not replicating")
                                        .build(),
                                DBInstanceStatusInfo.builder()
                                        .statusType("something else")
                                        .status("replicating")
                                        .build()
                        )
                        .build()
        ));
    }
}
