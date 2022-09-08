package software.amazon.rds.dbcluster;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    RdsClient rdsClient;
    @Mock
    Ec2Client ec2Client;
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    private ProxyClient<Ec2Client> ec2Proxy;

    @Getter
    private UpdateHandler handler;

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(
                HandlerConfig.builder()
                        .probingEnabled(false)
                        .backoff(Constant.of()
                                .delay(Duration.ofMillis(1))
                                .timeout(Duration.ofSeconds(120))
                                .build())
                        .build()
        );
        rdsClient = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
        ec2Proxy = MOCK_PROXY(proxy, ec2Client);
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SuccessRoleNotFound() {
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenThrow(DbClusterRoleNotFoundException.builder().message("not found").build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_RemoveFromGlobalCluster() {
        when(rdsProxy.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class)))
                .thenReturn(RemoveFromGlobalClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenThrow(DbClusterRoleNotFoundException.builder().message("not found").build());
        when(rdsProxy.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class)))
                .thenThrow(GlobalClusterNotFoundException.class);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("global-cluster-identifier").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class));
    }

    @Test
    public void handleRequest_RemoveFromGlobalClusterStabilization() {
        when(rdsProxy.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class)))
                .thenReturn(RemoveFromGlobalClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenThrow(DbClusterRoleNotFoundException.builder().message("not found").build());

        final Queue<GlobalCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(GlobalCluster.builder().globalClusterMembers(GlobalClusterMember.builder().dbClusterArn(DBCLUSTER_ACTIVE.dbClusterArn()).build()).build());
        transitions.add(GlobalCluster.builder().build());
        when(rdsProxy.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class)))
                .then(res -> DescribeGlobalClustersResponse.builder().globalClusters(transitions.remove()).build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("global-cluster-identifier").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(5)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class));
        verify(rdsProxy.client(), times(2)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));
    }

    @Test
    public void handleRequest_AddToGlobalClusterIsImmutable() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("global-cluster-identifier").build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_Success() {
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(6)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_SuccessWithEmptyVPC() {
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class)))
                .thenReturn(DescribeDbSubnetGroupsResponse.builder().dbSubnetGroups(DBSubnetGroup.builder().vpcId("vpcId").build()).build());
        when(ec2Proxy.client().describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
                .thenReturn(DescribeSecurityGroupsResponse.builder().securityGroups(SecurityGroup.builder().groupId("group-id").build()).build());
        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        final ResourceModel resourceModel = RESOURCE_MODEL_EMPTY_VPC.toBuilder().build();
        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> resourceModel,
                () -> resourceModel,
                expectSuccess()
        );

        Assertions.assertFalse(resourceModel.getVpcSecurityGroupIds().isEmpty());

        verify(rdsProxy.client(), times(6)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_HandleAssociatedRoleWithEmptyFeature() {
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();

        final DBCluster dbclusterActive = DBCLUSTER_ACTIVE.toBuilder()
                .associatedRoles(software.amazon.awssdk.services.rds.model.DBClusterRole.builder()
                        .roleArn(ROLE_WITH_EMPTY_FEATURE.getRoleArn())
                        .build())
                .build();

        transitions.add(dbclusterActive);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.builder(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return dbclusterActive;
                },
                () -> RESOURCE_MODEL.toBuilder().associatedRoles(Lists.newArrayList(ROLE_WITH_EMPTY_FEATURE)).build(),
                () -> RESOURCE_MODEL.toBuilder().associatedRoles(Lists.newArrayList(ROLE_WITH_EMPTY_FEATURE)).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(5)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
    }


    @Test
    public void handleRequest_ImmutableUpdate_GlobalCluster() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("globalClusterIdentifier_1").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("globalClusterIdentifier_2").build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_ImmutableUpdate_Engine() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().engine("engine_1").build(),
                () -> RESOURCE_MODEL.toBuilder().engine("engine_2").build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_NoMasterUserUpdateIfMatch() {
        final String masterUserPassword = randomString(16, ALPHANUM);

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder().masterUserPassword(masterUserPassword).build(),
                () -> RESOURCE_MODEL.toBuilder().masterUserPassword(masterUserPassword).build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterRequest> argument = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(argument.capture());
        Assertions.assertNull(argument.getValue().masterUserPassword());
    }

    @Test
    public void handleRequest_UpdateMasterUserPasswordIfMismatch() {
        final String masterUserPassword1 = randomString(16, ALPHANUM);
        final String masterUserPassword2 = randomString(16, ALPHANUM);

        Assertions.assertNotEquals(masterUserPassword1, masterUserPassword2);

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder().masterUserPassword(masterUserPassword1).build(),
                () -> RESOURCE_MODEL.toBuilder().masterUserPassword(masterUserPassword2).build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterRequest> argument = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(argument.capture());
        Assertions.assertEquals(argument.getValue().masterUserPassword(), masterUserPassword2);
    }

    @Test
    public void handleRequest_NoEngineVersionUpdateIfMatch() {
        final String engineVersion = randomString(16, ALPHANUM);

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion).build(),
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion).build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterRequest> argument = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(argument.capture());
        Assertions.assertNull(argument.getValue().engineVersion());
    }

    @Test
    public void handleRequest_NoEngineVersionUpdateIfRollback() {
        final String engineVersion1 = randomString(16, ALPHANUM);
        final String engineVersion2 = randomString(16, ALPHANUM);

        Assertions.assertNotEquals(engineVersion1, engineVersion2);

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion1).build(),
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion2).build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterRequest> argument = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(argument.capture());
        Assertions.assertNull(argument.getValue().engineVersion());
    }

    @Test
    public void handleRequest_EngineVersionUpdateIfMismatch() {
        final String engineVersion1 = randomString(16, ALPHANUM);
        final String engineVersion2 = randomString(16, ALPHANUM);

        Assertions.assertNotEquals(engineVersion1, engineVersion2);

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion1).build(),
                () -> RESOURCE_MODEL.toBuilder().engineVersion(engineVersion2).build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterRequest> argument = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(argument.capture());
        Assertions.assertEquals(argument.getValue().engineVersion(), engineVersion2);
        Assertions.assertTrue(argument.getValue().applyImmediately());
        Assertions.assertTrue(argument.getValue().allowMajorVersionUpgrade());
    }

    static class ModifyDBClusterExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    // Put exception classes below
                    Arguments.of(DbClusterNotFoundException.builder().message(ERROR_MSG).build(), HandlerErrorCode.NotFound)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ModifyDBClusterExceptionArgumentsProvider.class)
    public void handleRequest_ModifyDBCluster_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectModifyDBClusterCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                requestException,
                expectResponseCode
        );
    }
}
