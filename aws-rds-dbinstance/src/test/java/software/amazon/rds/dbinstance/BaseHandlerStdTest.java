package software.amazon.rds.dbinstance;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceStatusInfo;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.MasterUserSecret;
import software.amazon.awssdk.services.rds.model.PendingCloudwatchLogsExports;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.awssdk.services.rds.model.ProcessorFeature;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.MOCK_CREDENTIALS;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.logger;
import static software.amazon.rds.dbinstance.AbstractHandlerTest.mockProxy;
import static software.amazon.rds.dbinstance.status.DBParameterGroupStatus.Applying;
import static software.amazon.rds.dbinstance.status.DBParameterGroupStatus.InSync;
import static software.amazon.rds.dbinstance.status.DBParameterGroupStatus.PendingReboot;

class BaseHandlerStdTest {

    static class TestBaseHandlerStd extends BaseHandlerStd {

        public TestBaseHandlerStd(HandlerConfig config) {
            super(config);
            requestLogger= new RequestLogger(logger, ResourceHandlerRequest.builder().build(), null);
        }

        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
                AmazonWebServicesClientProxy proxy,
                ValidatedRequest<ResourceModel> request,
                CallbackContext context,
                VersionedProxyClient<RdsClient> rdsProxyClient,
                VersionedProxyClient<Ec2Client> ec2ProxyClient
        ) {
            return null;
        }
    }

    private TestBaseHandlerStd handler;

    @Mock
    private ProxyClient<RdsClient> rdsProxyV12;

    @BeforeEach
    public void setUp() {
        handler = new TestBaseHandlerStd(null);
        AmazonWebServicesClientProxy proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        RdsClient rdsClientV12 = mock(RdsClient.class);
        rdsProxyV12 = mockProxy(proxy, rdsClientV12);
    }

    @Test
    void isDomainMembershipsJoined_NullDomainMembershipReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isDomainMembershipsJoined(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isDomainMembershipsJoined_EmptyDomainMembershipReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isDomainMembershipsJoined(
                DBInstance.builder()
                        .domainMemberships(Collections.emptyList())
                        .build()
        )).isTrue();
    }

    @Test
    void isDomainMembershipsJoined_NonEmptyListJoinedAndKerberosReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isDomainMembershipsJoined(
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
        Assertions.assertThat(DBInstancePredicates.isDomainMembershipsJoined(
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
        Assertions.assertThat(DBInstancePredicates.isVpcSecurityGroupsActive(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isVpcSecurityGroupsActive_EmptyVpcSecurityGroupsReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isVpcSecurityGroupsActive(
                DBInstance.builder()
                        .vpcSecurityGroups(Collections.emptyList())
                        .build()
        )).isTrue();
    }

    @Test
    void isVpcSecurityGroupsActive_NonEmptyVpcSecurityGroupsActiveReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isVpcSecurityGroupsActive(
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
        Assertions.assertThat(DBInstancePredicates.isVpcSecurityGroupsActive(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_EmptyPendingChangesReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(PendingModifiedValues.builder().build())
                        .build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_NonEmptyAllocatedStorageReturnsFalse() {
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .allocatedStorage(42)
                                        .build())
                        .build()
        )).isFalse();
    }

    @Test
    void isCaCertificateChangesApplied_NonEmptyCACertificateIdentifierReturnsFalse_WhenCertificateRotationRestartIsTrue() {
        Assertions.assertThat(DBInstancePredicates.isCaCertificateChangesApplied(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .caCertificateIdentifier("certificate")
                                        .build())
                        .build(),
                ResourceModel.builder().certificateRotationRestart(true).build()
        )).isFalse();
    }

    @Test
    void isCaCertificateChangesApplied_NonEmptyCACertificateIdentifierReturnsTrue_WhenCertificateRotationRestartIsFalse() {
        Assertions.assertThat(DBInstancePredicates.isCaCertificateChangesApplied(
                DBInstance.builder()
                        .pendingModifiedValues(
                                PendingModifiedValues.builder()
                                        .caCertificateIdentifier("certificate")
                                        .build())
                        .build(),
                ResourceModel.builder().certificateRotationRestart(false).build()
        )).isTrue();
    }

    @Test
    void isNoPendingChanges_NonEmptyMasterUserPasswordReturnsFalse() {
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isNoPendingChanges(
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
        Assertions.assertThat(DBInstancePredicates.isDBParameterGroupNotApplying(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isDBParameterGroupSyncComplete_EmptyParameterGroupsReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isDBParameterGroupNotApplying(
                DBInstance.builder().dbParameterGroups(Collections.emptyList()).build()
        )).isTrue();
    }

    @Test
    void isDBParameterGroupSyncComplete_NonEmptyParameterGroupsInSyncReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isDBParameterGroupNotApplying(
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
        Assertions.assertThat(DBInstancePredicates.isDBParameterGroupNotApplying(
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
        Assertions.assertThat(DBInstancePredicates.isReplicationComplete(
                DBInstance.builder().build()
        )).isTrue();
    }

    @Test
    void isReplicationComplete_EmptyStatusInfoReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isReplicationComplete(
                DBInstance.builder().statusInfos(Collections.emptyList()).build()
        )).isTrue();
    }

    @Test
    void isReplicationComplete_NonEmptyListNoReadReplicaInfoReturnsTrue() {
        Assertions.assertThat(DBInstancePredicates.isReplicationComplete(
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
        Assertions.assertThat(DBInstancePredicates.isReplicationComplete(
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
        Assertions.assertThat(DBInstancePredicates.isReplicationComplete(
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
    void isMasterUserSecretStabilized_masterUserSecretIsNull() {
        Assertions.assertThat(DBInstancePredicates.isMasterUserSecretStabilized(
                DBInstance.builder()
                        .build()
        )).isTrue();
    }

    @Test
    void isMasterUserSecretStabilized_masterUserSecretStatusActive() {
        Assertions.assertThat(DBInstancePredicates.isMasterUserSecretStabilized(
                DBInstance.builder()
                        .masterUserSecret(MasterUserSecret.builder()
                                .secretStatus("Active")
                                .build())
                        .build()
        )).isTrue();
    }

    @Test
    void isMasterUserSecretStabilized_masterUserSecretStatusCreating() {
        Assertions.assertThat(DBInstancePredicates.isMasterUserSecretStabilized(
                DBInstance.builder()
                        .masterUserSecret(MasterUserSecret.builder()
                                .secretStatus("Creating")
                                .build())
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

    private static Stream<Arguments> DBClusterParameterGroupTestCases() {
        return Stream.of(
            // Test cases in the format: paramStatus, applyImmediately, expectedStabilizationState
            // given the DBParameterGroup status and applyImmediately flag, it determines whether the resource should stabilize
            Arguments.of(Applying.toString(), false, false),
            Arguments.of(Applying.toString(), true, false),
            Arguments.of(InSync.toString(), false, true),
            Arguments.of(InSync.toString(), true, true),
            Arguments.of(PendingReboot.toString(), false, true),
            Arguments.of(PendingReboot.toString(), true, false)
        );
    }

    @ParameterizedTest()
    @MethodSource("DBClusterParameterGroupTestCases")
    void isDBClusterParameterGroupStabilizedTests(String paramStatus, Boolean applyImmediately, boolean expectedStabilizationState) {
        String dbIdentifier = "testDb";

        var dbClusterWithMember = DBCluster.builder()
            .dbClusterMembers(
                (DBClusterMember.builder()
                    .dbInstanceIdentifier(dbIdentifier)
                    .dbClusterParameterGroupStatus(paramStatus)
                    .build())
            ).build();

        final ResourceModel model = ResourceModel.builder()
            .dBInstanceIdentifier(dbIdentifier)
            .applyImmediately(applyImmediately)
            .build();

        when(rdsProxyV12.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
            .thenReturn(DescribeDbClustersResponse.builder()
                .dbClusters(dbClusterWithMember)
                .build());


        boolean actual = handler.isDBClusterParameterGroupStabilized(rdsProxyV12, model);

        Assertions.assertThat(actual).isEqualTo(expectedStabilizationState);
    }

    private static Stream<Arguments> DBParameterGroupTestCases() {
        return Stream.of(
            // Test cases in the format: paramStatus, applyImmediately, expectedStabilizationState
            // given the DBParameterGroup status and applyImmediately flag, it determines whether the resource should stabilize
                Arguments.of(Applying.toString(), false, false),
                Arguments.of(Applying.toString(), true, false),
                Arguments.of(InSync.toString(), false, true),
                Arguments.of(InSync.toString(), true, true),
                Arguments.of(PendingReboot.toString(), false, true),
                Arguments.of(PendingReboot.toString(), true, false)
        );
    }

    @ParameterizedTest()
    @MethodSource("DBParameterGroupTestCases")
    void isDBParameterGroupStabilizedTests(String paramStatus, Boolean applyImmediately, boolean expectedStabilizationState) {
        String dbIdentifier = "testDb";

        var dbInstance = DBInstance.builder()
            .dbParameterGroups(DBParameterGroupStatus.builder()
                .dbParameterGroupName("test")
                .parameterApplyStatus(paramStatus)
                .build())
            .build();

        final ResourceModel model = ResourceModel.builder()
            .dBInstanceIdentifier(dbIdentifier)
            .applyImmediately(applyImmediately)
            .build();

        when(rdsProxyV12.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
            .thenReturn(DescribeDbInstancesResponse.builder()
                .dbInstances(List.of(dbInstance))
                .build());

        boolean actual = handler.isDBParameterGroupStabilized(rdsProxyV12, model);

        Assertions.assertThat(actual).isEqualTo(expectedStabilizationState);
    }
}
