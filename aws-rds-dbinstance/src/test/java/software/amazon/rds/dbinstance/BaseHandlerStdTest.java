package software.amazon.rds.dbinstance;

import java.time.Instant;
import java.util.Collections;

import org.assertj.core.api.Assertions;
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
import software.amazon.rds.dbinstance.request.RequestValidationException;
import software.amazon.rds.dbinstance.request.ValidatedRequest;

class BaseHandlerStdTest {

    class TestBaseHandlerStd extends BaseHandlerStd {

        public TestBaseHandlerStd(HandlerConfig config) {
            super(config);
        }

        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
                AmazonWebServicesClientProxy proxy,
                ValidatedRequest<ResourceModel> request,
                CallbackContext context,
                VersionedProxyClient<RdsClient> rdsProxyClient,
                VersionedProxyClient<Ec2Client> ec2ProxyClient,
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
    void isPromotionTierUpdated_NullPromotionTierReturnsTrue() {
        Assertions.assertThat(handler.isPromotionTierUpdated(
                DBInstance.builder().build(),
                ResourceModel.builder().build()
        )).isTrue();
    }

    @Test
    void isPromotionTierUpdated_MatchingPromotionTiedReturnsTrue() {
        Assertions.assertThat(handler.isPromotionTierUpdated(
                DBInstance.builder().promotionTier(42).build(),
                ResourceModel.builder().promotionTier(42).build()
        )).isTrue();
    }

    @Test
    void isPromotionTierUpdated_EmptyModelPromotionTierReturnsTrue() {
        Assertions.assertThat(handler.isPromotionTierUpdated(
                DBInstance.builder().promotionTier(42).build(),
                ResourceModel.builder().promotionTier(null).build()
        )).isTrue();
    }

    @Test
    void isPromotionTierUpdated_MismatchingPromotionTierReturnsFalse() {
        Assertions.assertThat(handler.isPromotionTierUpdated(
                DBInstance.builder().promotionTier(null).build(),
                ResourceModel.builder().promotionTier(42).build()
        )).isFalse();
    }

    @Test
    void isDomainMembershipsJoined_NullDomainMembershipReturnsTrue() {
        Assertions.assertThat(handler.isDomainMembershipsJoined(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isDomainMembershipsJoined_EmptyDomainMembershipReturnsTrue() {
        Assertions.assertThat(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(Collections.emptyList())
                        .build()
        )).isTrue();
    }

    @Test
    void isDomainMembershipsJoined_NonEmptyListJoinedAndKerberosReturnsTrue() {
        Assertions.assertThat(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(
                                DomainMembership.builder().status("joined").build(),
                                DomainMembership.builder().status("kerberos-enabled").build()
                        )
                        .build()
        )).isTrue();
    }

    @Test
    void isDomainMembershipsJoined_NonEmptyListJoinedAndKerberosAndAnythingElseReturnsFalse() {
        Assertions.assertThat(handler.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(
                                DomainMembership.builder().status("joined").build(),
                                DomainMembership.builder().status("kerberos-enabled").build(),
                                DomainMembership.builder().status("something else").build()
                        )
                        .build()
        )).isFalse();
    }

    @Test
    void isVpcSecurityGroupsActive_NullVpcSecurityGroupsReturnsTrue() {
        Assertions.assertThat(handler.isVpcSecurityGroupsActive(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isVpcSecurityGroupsActive_EmptyVpcSecurityGroupsReturnsTrue() {
        Assertions.assertThat(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(Collections.emptyList())
                        .build()
        )).isTrue();
    }

    @Test
    void isVpcSecurityGroupsActive_NonEmptyVpcSecurityGroupsActiveReturnsTrue() {
        Assertions.assertThat(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(
                                VpcSecurityGroupMembership.builder().status("active").build(),
                                VpcSecurityGroupMembership.builder().status("active").build()
                        )
                        .build()
        )).isTrue();
    }

    @Test
    void isVpcSecurityGroupsActive_NonEmptyVpcSecurityGroupsNotActiveReturnsFalse() {
        Assertions.assertThat(handler.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(
                                VpcSecurityGroupMembership.builder().status("active").build(),
                                VpcSecurityGroupMembership.builder().status(null).build()
                        )
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NullPendingChangesReturnsTrue() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_EmptyPendingChangesReturnsTrue() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(PendingModifiedValues.builder().build())
                        .build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_NonEmptyAllocatedStorageReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .allocatedStorage(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyCACertificateIdentifierReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .caCertificateIdentifier("certificate")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyMasterUserPasswordReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .masterUserPassword("password")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyBackupRetentionPeriodReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .backupRetentionPeriod(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyMultiAZReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .multiAZ(true)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyEngineVersionReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .engineVersion("1.2.3")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyIopsReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .iops(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyDBInstanceIdentifierReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .dbInstanceIdentifier("dbinstance")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyLicenseModelReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .licenseModel("license model")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyStorageTypeReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .storageType("storage type")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyDBSubnetGroupNameReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .dbSubnetGroupName("db-subnet-group-name")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyPendingCloudWatchLogsExportsReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .pendingCloudwatchLogsExports(PendingCloudwatchLogsExports.builder().build())
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_EmptyProcessorFeaturesReturnsTrue() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .processorFeatures(Collections.emptyList())
                                        .build())
                        .build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_NonEmptyIamDatabaseAutenticationEnabledReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .iamDatabaseAuthenticationEnabled(true)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyAutomationModeReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .automationMode("automation mode")
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyResumeFullAutomationModeTimeReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .resumeFullAutomationModeTime(Instant.now())
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isNoPendingChanges_NonEmptyReturnsFalse() {
        Assertions.assertThat(handler.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .processorFeatures(ProcessorFeature.builder().build())
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isDBParameterGroupSyncComplete_NullParameterGroupsReturnsTrue() {
        Assertions.assertThat(handler.isDBParameterGroupNotApplying(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isDBParameterGroupSyncComplete_EmptyParameterGroupsReturnsTrue() {
        Assertions.assertThat(handler.isDBParameterGroupNotApplying(
                DBInstance.builder().dbParameterGroups(Collections.emptyList()).build()
        )).isTrue();
    }

    @Test
    void isDBParameterGroupSyncComplete_NonEmptyParameterGroupsInSyncReturnsTrue() {
        Assertions.assertThat(handler.isDBParameterGroupNotApplying(
                DBInstance.builder()
                        .dbParameterGroups(
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build(),
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build()
                        )
                        .build()
        )).isTrue();
    }

    @Test
    void isDBParameterGroupSyncComplete_NonEmptyParameterGroupsApplyingReturnsFalse() {
        Assertions.assertThat(handler.isDBParameterGroupNotApplying(
                DBInstance.builder()
                        .dbParameterGroups(
                                DBParameterGroupStatus.builder().parameterApplyStatus("in-sync").build(),
                                DBParameterGroupStatus.builder().parameterApplyStatus("applying").build()
                        )
                        .build()
        )).isFalse();
    }

    @Test
    void isReplicationComplete_NullStatusInfoReturnsTrue() {
        Assertions.assertThat(handler.isReplicationComplete(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isReplicationComplete_EmptyStatusInfoReturnsTrue() {
        Assertions.assertThat(handler.isReplicationComplete(
                DBInstance.builder().statusInfos(Collections.emptyList()).build()
        )).isTrue();
    }

    @Test
    void isReplicationComplete_NonEmptyListNoReadReplicaInfoReturnsTrue() {
        Assertions.assertThat(handler.isReplicationComplete(
                DBInstance.builder()
                        .statusInfos(DBInstanceStatusInfo.builder()
                                .statusType("something else")
                                .status("not replicating")
                                .build())
                        .build()
        )).isTrue();
    }

    @Test
    void isReplicationComplete_NonEmptyListContainingReplicaInfoReplicatingReturnsTrue() {
        Assertions.assertThat(handler.isReplicationComplete(
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
        )).isTrue();
    }

    @Test
    void isReplicationComplete_NonEmptyListContainingReplicaInfoNotReplicatingReturnsFalse() {
        Assertions.assertThat(handler.isReplicationComplete(
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
        )).isFalse();
    }

    @Test
    void validateRequest_BlankRegionIsAccepted() {
        final ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
        request.setDesiredResourceState(ResourceModel.builder()
                .sourceRegion("")
                .build());
        Assertions.assertThatCode(() -> {
            handler.validateRequest(request);
        }).doesNotThrowAnyException();
    }

    @Test
    void validateRequest_UnknownRegionIsRejected() {
        final ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
        request.setDesiredResourceState(ResourceModel.builder()
                .sourceRegion("foo-bar-baz")
                .build());
        Assertions.assertThatExceptionOfType(RequestValidationException.class).isThrownBy(() -> {
            handler.validateRequest(request);
        });
    }
}
