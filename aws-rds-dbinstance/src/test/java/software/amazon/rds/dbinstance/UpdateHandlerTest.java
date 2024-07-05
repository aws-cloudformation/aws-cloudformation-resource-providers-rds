package software.amazon.rds.dbinstance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.rds.dbinstance.BaseHandlerStd.API_VERSION_V12;
import static software.amazon.rds.dbinstance.BaseHandlerStd.RESOURCE_UPDATED_AT;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.util.SdkAutoConstructList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackupsReplication;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.PromoteReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.PromoteReadReplicaResponse;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.StartDbInstanceAutomatedBackupsReplicationRequest;
import software.amazon.awssdk.services.rds.model.StopDbInstanceAutomatedBackupsReplicationRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    private ProxyClient<RdsClient> rdsProxyV12;

    @Mock
    @Getter
    private ProxyClient<Ec2Client> ec2Proxy;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private RdsClient rdsClientV12;

    @Mock
    private Ec2Client ec2Client;

    @Getter
    private UpdateHandler handler;

    private Boolean expectServiceInvocation;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @Override
    public ProxyClient<RdsClient> getRdsProxy(final String version) {
        switch (version) {
            case API_VERSION_V12:
                return rdsProxyV12;
            default:
                return rdsProxy;
        }
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(HandlerConfig.builder()
                .probingEnabled(false)
                .backoff(TEST_BACKOFF_DELAY)
                .build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        rdsClientV12 = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);

        rdsProxy = mockProxy(proxy, rdsClient);
        rdsProxyV12 = mockProxy(proxy, rdsClientV12);
        ec2Proxy = mockProxy(proxy, ec2Client);

        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
        verifyAccessPermissions(rdsClient);
        verifyAccessPermissions(rdsClientV12);
        verifyAccessPermissions(ec2Client);
    }

    @Test
    public void handleRequest_modifyDbInstance_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client()).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_modifyDbInstanceV12_Success() {
        when(rdsProxyV12.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxyV12.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        .dBSecurityGroups(DB_SECURITY_GROUPS)
                        .build(),
                () -> RESOURCE_MODEL_ALTER.toBuilder()
                        .dBSecurityGroups(DB_SECURITY_GROUPS_ALTER)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxyV12.client(), times(1)).modifyDBInstance(argumentCaptor.capture());
        verify(rdsProxyV12.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client()).describeEvents(any(DescribeEventsRequest.class));

        Assertions.assertThat(argumentCaptor.getValue().dbSecurityGroups()).containsExactly(Iterables.toArray(DB_SECURITY_GROUPS_ALTER, String.class));
    }

    @Test
    public void handleRequest_UnsetMaxAllocatedStorage() {
        final ModifyDbInstanceResponse modifyDbInstanceResponse = ModifyDbInstanceResponse.builder()
                .dbInstance(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyDbInstanceResponse);

        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        .allocatedStorage(ALLOCATED_STORAGE.toString())
                        .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .allocatedStorage(ALLOCATED_STORAGE.toString())
                        .maxAllocatedStorage(null)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbInstanceRequest> argument = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(argument.capture());
        Assertions.assertThat(argument.getValue().maxAllocatedStorage()).isEqualTo(ALLOCATED_STORAGE);

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_SuccessTagsAddOnly() {
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        List<Tag> updatedTags = new ArrayList<>(TAG_LIST);
        updatedTags.add(Tag.builder().key("updated").value("tag").build());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().tags(updatedTags).build(),
                expectSuccess()
        );

        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_ResourceTagsPrioritizedOverStackTags() {
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        List<Tag> updatedTags = new ArrayList<>(TAG_LIST);
        updatedTags.add(Tag.builder().key("tag-key").value("resource-level").build());

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Collections.emptyMap())
                        .desiredResourceTags(Translator.translateTagsToRequest(Collections.singleton(
                                Tag.builder().key("tag-key").value("stack-level").build()
                        ))),
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().tags(updatedTags).build(),
                expectSuccess()
        );

        final ArgumentCaptor<AddTagsToResourceRequest> captor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client()).addTagsToResource(captor.capture());
        Assertions.assertThat(captor.getValue().tags())
                .containsExactlyInAnyOrder(
                        software.amazon.awssdk.services.rds.model.Tag .builder().key("tag-key").value("resource-level").build());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SuccessTagsRemoveOnly() {
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().tags(Collections.emptyList()).build(),
                expectSuccess()
        );

        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_StackLevelTagsRemovalDoesNotRemoveTagsIfTheyExistOnResourceLevel() {
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Collections.emptyMap()),
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_FallBackToStackLevelTagsIfResourceLevelWasRemoved() {
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(Collections.singleton(
                                Tag.builder().key("tag-key").value("stack-level").build())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Collections.singleton(
                                Tag.builder().key("tag-key").value("stack-level").build()))),
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().tags(Collections.singletonList(Tag.builder().key("tag-key").value("resource-level").build())).build(),
                () -> RESOURCE_MODEL_BLDR().tags(Collections.emptyList()).build(),
                expectSuccess()
        );

        final ArgumentCaptor<AddTagsToResourceRequest> captor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client()).addTagsToResource(captor.capture());
        Assertions.assertThat(captor.getValue().tags())
                .containsExactlyInAnyOrder(
                        software.amazon.awssdk.services.rds.model.Tag .builder().key("tag-key").value("stack-level").build());
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DeleteNonExistingRole() {
        // compute a complete sequence of transitions from the initial set of roles to the final one.
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, ASSOCIATED_ROLES, ASSOCIATED_ROLES_ALTER)
        );
        // We expect describeDBInstances to be called 2 more times: for tag mutation and for the final resource fetch.
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES_ALTER))
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES_ALTER))
                .build());

        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenThrow(DbInstanceRoleNotFoundException.class);
        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateAlreadyExistingRole() {
        // compute a complete sequence of transitions from the initial set of roles to the final one.
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, ASSOCIATED_ROLES, ASSOCIATED_ROLES_ALTER)
        );
        // We expect describeDBInstances to be called 2 more times: for tag mutation and for the final resource fetch.
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES_ALTER))
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES_ALTER))
                .build());

        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class)))
                .thenThrow(DbInstanceRoleAlreadyExistsException.class);
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);

        test_handleRequest_base(
                context,
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_UpdateRoles_InternalExceptionOnAdd() {
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).then(res -> {
            throw new RuntimeException(MSG_GENERIC_ERR);
        });

        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(3)).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxy.client()).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_UpdateRolesInternalExceptionOnRemove() {
        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).then(res -> {
            throw new RuntimeException(MSG_GENERIC_ERR);
        });

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client()).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_UpdateRolesInternalExceptionOnTagging() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, ASSOCIATED_ROLES, ASSOCIATED_ROLES_ALTER)
        );

        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);
        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(false);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> {
                    if (!transitions.isEmpty()) {
                        return transitions.remove();
                    }
                    throw new RuntimeException(MSG_GENERIC_ERR);
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(2)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxy.client(), times(7)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_UpdateRolesAndTags() {
        // compute a complete sequence of transitions from the initial set of roles to the final one.
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, ASSOCIATED_ROLES, ASSOCIATED_ROLES_ALTER)
        );
        // We expect describeDBInstances to be called 3 more times: for tag mutation, reboot check and the final resource fetch.
        for (int i = 0; i < 3; i++) {
            transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                    .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES_ALTER))
                    .build());
        }

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);
        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);
        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxy.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                expectSuccess()
        );

        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(2)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxy.client(), times(8)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_ShouldReboot_Success() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();

        transitions.add(DB_INSTANCE_ACTIVE.toBuilder().dbParameterGroups(
                ImmutableList.of(DBParameterGroupStatus.builder()
                        .dbParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                        .parameterApplyStatus(software.amazon.rds.dbinstance.status.DBParameterGroupStatus.PendingReboot.toString())
                        .build())
        ).build());

        transitions.add(DB_INSTANCE_ACTIVE.toBuilder().dbParameterGroups(
                ImmutableList.of(DBParameterGroupStatus.builder()
                        .dbParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                        .parameterApplyStatus(software.amazon.rds.dbinstance.status.DBParameterGroupStatus.InSync.toString())
                        .build())
        ).build());

        transitions.add(DB_INSTANCE_ACTIVE);

        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxy.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(false);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_NoEngineVersionChangeOnRollback() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .engineVersion(ENGINE_VERSION_MYSQL_80)
                .build();

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        // Ensure that engineVersion is not set on a rollback, otherwise it will fail the attempt.
        final ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(captor.capture());
        Assertions.assertThat(captor.getValue().engineVersion()).isNull();

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_SetParameterGroupName() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of(DBParameterGroup.builder().build()))
                .build();
        when(rdsProxy.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final DescribeDbEngineVersionsResponse describeDbEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(DBEngineVersion.builder().build())
                .build();
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class))).thenReturn(describeDbEngineVersionsResponse);

        // Altering the db parameter group name attribute invokes setParameterGroupName
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_ALTER)
                .engineVersion(ENGINE_VERSION_MYSQL_80)
                .build();
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true); // this is an emulation of a re-entrance
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(rdsProxy.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SetParameterGroupName_EmptyDbParameterGroupName() {
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(null)
                .engineVersion(ENGINE_VERSION_MYSQL_80)
                .build();
        // An empty db parameter group name will cause setParameterGroupName to return earlier
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true); // this is an emulation of a re-entrance
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_PerformanceInsightsKMSId_UpdatedFromEmptyToValue() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .performanceInsightsKMSKeyId("performance_insights_kms_id")
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);

        verify(rdsProxy.client(), times(1)).modifyDBInstance(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().performanceInsightsKMSKeyId()).isEqualTo("performance_insights_kms_id");

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_PerformanceInsightsKMSId_UpdatedToDifferentValue() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .performanceInsightsKMSKeyId("performance_insights_kms_id")
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .performanceInsightsKMSKeyId("old-performance_insights_kms_id")
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_SetParameterGroupName_NoDbParameterGroups() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of()) // empty db parameter group set
                .build();
        when(rdsProxy.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        // Altering the db parameter group name attribute invokes setParameterGroupName
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_ALTER)
                .engineVersion(ENGINE_VERSION_MYSQL_80)
                .build();
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true); // this is an emulation of a re-entrance
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SetParameterGroupName_EmptyDbEngineVersions() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of(DBParameterGroup.builder().build()))
                .build();
        when(rdsProxy.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final DescribeDbEngineVersionsResponse describeDbEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(ImmutableList.of()) // empty list
                .build();
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class))).thenReturn(describeDbEngineVersionsResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true); // this is an emulation of a re-entrance
        context.setStorageAllocated(true);


        // Altering the db parameter group name attribute invokes setParameterGroupName
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_ALTER)
                .engineVersion(ENGINE_VERSION_MYSQL_80)
                .build();
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(rdsProxy.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SetDefaultVpcId() {
        final DescribeSecurityGroupsResponse describeSecurityGroupsResponse = DescribeSecurityGroupsResponse.builder()
                .securityGroups(SecurityGroup.builder().groupName(DB_SECURITY_GROUP_DEFAULT).groupId(DB_SECURITY_GROUP_ID).build())
                .build();
        when(ec2Proxy.client().describeSecurityGroups(any(DescribeSecurityGroupsRequest.class))).thenReturn(describeSecurityGroupsResponse);

        final CallbackContext context = new CallbackContext();
        context.setUpdated(true); // this is an emulation of a re-entrance
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE.toBuilder().dbSubnetGroup(
                        DBSubnetGroup.builder().vpcId(DB_SECURITY_GROUP_VPC_ID).build()
                ).build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR()
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(ec2Proxy.client()).describeSecurityGroups(any(DescribeSecurityGroupsRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_NoDefaultVpcIdForClusterInstance() {
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        // A default vpc group won't be set for a db cluster member
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_NoDefaultVpcIdForOracleCustom() {
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        // A default vpc group won't be set for rds oracle custom instance
                        .engine("custom-oracle-ee-cdb")
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .engine("custom-oracle-ee-cdb")
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_ResourceDrift() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("pending-reboot")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("foo")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus("available")
                .build());

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().driftable(true),
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier(null).build(),
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier(null).build(),
                expectSuccess()
        );


        verify(rdsProxy.client(), times(6)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).rebootDBInstance(any(RebootDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_ResourceDriftClusterInstance() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("pending-reboot")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("foo")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("foo")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus("available")
                .build());

        when(rdsProxy.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder()
                                .dbClusterMembers(DBClusterMember.builder()
                                        .dbInstanceIdentifier(DB_INSTANCE_ACTIVE.dbInstanceIdentifier())
                                        .dbClusterParameterGroupStatus("applying")
                                        .build())
                                .build())
                        .build())
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder()
                                .dbClusterMembers(DBClusterMember.builder()
                                        .dbInstanceIdentifier(DB_INSTANCE_ACTIVE.dbInstanceIdentifier())
                                        .dbClusterParameterGroupStatus("in-sync")
                                        .build())
                                .build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().driftable(true),
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier("db-cluster-identifier").build(),
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier("db-cluster-identifier").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(7)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).rebootDBInstance(any(RebootDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_ResourceDriftClusterInstanceShouldRestartCluster() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("foo")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("applying")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .optionGroupName("test-option-group")
                        .status("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("test-db-parameter-group")
                        .parameterApplyStatus("in-sync")
                        .build())
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus("available")
                .build());
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus("available")
                .build());

        when(rdsProxy.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder()
                                .dbClusterMembers(DBClusterMember.builder()
                                        .dbInstanceIdentifier(DB_INSTANCE_ACTIVE.dbInstanceIdentifier())
                                        .dbClusterParameterGroupStatus("pending-reboot")
                                        .build())
                                .build())
                        .build())
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder()
                                .dbClusterMembers(DBClusterMember.builder()
                                        .dbInstanceIdentifier(DB_INSTANCE_ACTIVE.dbInstanceIdentifier())
                                        .dbClusterParameterGroupStatus("in-sync")
                                        .build())
                                .build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().driftable(true),
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier("db-cluster-identifier").build(),
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier("db-cluster-identifier").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxy.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_EmptyVpcSecurityGroupIdList() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setUpdated(false);
        context.setStorageAllocated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        // A default vpc group won't be set for a db cluster member
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(captor.capture());
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isEmpty();
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isInstanceOf(SdkAutoConstructList.class);

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    static class ModifyDBInstanceExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.InvalidDBInstanceState, HandlerErrorCode.ResourceConflict),
                    Arguments.of(ErrorCode.InvalidDBSecurityGroupState, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.InvalidParameterCombination, HandlerErrorCode.InvalidRequest)
                    // Put exception classes below
                    // <empty>
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ModifyDBInstanceExceptionArgumentProvider.class)
    public void handleRequest_ModifyDBInstance_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        final CallbackContext context = new CallbackContext();
        context.setStorageAllocated(true);

        test_handleRequest_error(
                expectModifyDBInstanceCall(),
                context,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_ALTER,
                requestException,
                expectResponseCode
        );
    }

    @Test
    public void handleRequest_StorageFull() {
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();

        transitions.add(DB_INSTANCE_STORAGE_FULL);
        transitions.add(DB_INSTANCE_ACTIVE);
        transitions.add(DB_INSTANCE_ACTIVE); // one extra for the read handler

        test_handleRequest_base(
                context,
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().
                        allocatedStorage(ALLOCATED_STORAGE_INCR.toString()).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client()).modifyDBInstance(captor.capture());

        final ModifyDbInstanceRequest expected = ModifyDbInstanceRequest.builder()
                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .allocatedStorage(ALLOCATED_STORAGE_INCR)
                .applyImmediately(true)
                .build();

        Assertions.assertThat(captor.getValue().equalsBySdkFields(expected)).isTrue();
    }

    @Test
    public void handleRequest_StorageIsNotFull() {
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().
                        allocatedStorage(ALLOCATED_STORAGE_INCR.toString()).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_StorageFullRequestFetchingInstanceFails() {
        expectServiceInvocation = false;
        final CallbackContext context = new CallbackContext();
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SetEngineVersionIfChanged() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final String previousEngineVersion = ENGINE_VERSION_MYSQL_56;
        final String desiredEngineVersion = ENGINE_VERSION_MYSQL_80;

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().engineVersion(previousEngineVersion).build(),
                () -> RESOURCE_MODEL_BLDR().engineVersion(desiredEngineVersion).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        final ArgumentCaptor<ModifyDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().engineVersion()).isEqualTo(desiredEngineVersion);

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_UnsetEngineVersionIfNoChange() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final String previousEngineVersion = ENGINE_VERSION_MYSQL_56;
        final String desiredEngineVersion = previousEngineVersion;

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().engineVersion(previousEngineVersion).build(),
                () -> RESOURCE_MODEL_BLDR().engineVersion(desiredEngineVersion).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        final ArgumentCaptor<ModifyDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().engineVersion()).isNull();

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_PromoteReadReplica() {
        when(rdsProxy.client().promoteReadReplica(any(PromoteReadReplicaRequest.class)))
                .thenReturn(PromoteReadReplicaResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setUpdated(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().sourceDBInstanceIdentifier("previous").build(),
                () -> RESOURCE_MODEL_BLDR().sourceDBInstanceIdentifier(null).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client()).promoteReadReplica(any(PromoteReadReplicaRequest.class));
    }

    @Test
    public void handleRequest_UseLatestRestorableTime_UpdatedFromTrueToFalse() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(false)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(true)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_UseLatestRestorableTime_UpdatedFromTrueToEmpty() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(null)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(true)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_PerformanceInsightsKMSId_UpdatedFromFalseToTrue() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(true)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .useLatestRestorableTime(false)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_RestoreTime_UpdatedFromValueToEmpty() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .restoreTime(null)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .restoreTime(RESTORE_TIME_UTC)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_RestoreTime_UpdatedToDifferentValue() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .restoreTime(RESTORE_TIME_UTC_PLUS_5)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .restoreTime(RESTORE_TIME_UTC)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_SourceDBInstanceAutomatedBackupsArn_UpdatedFromValueToEmpty() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .sourceDBInstanceAutomatedBackupsArn(null)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .sourceDBInstanceAutomatedBackupsArn(SOURCE_DB_INSTANCE_AUTOMATED_BACKUPS_ARN_NON_EMPTY)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_FetchEventsFromUpdateMoment() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setUpdated(false);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final Instant updatedAt = response.getCallbackContext().getTimestamp(RESOURCE_UPDATED_AT);
        Assertions.assertThat(updatedAt).isNotNull();

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        ArgumentCaptor<DescribeEventsRequest> describeEventsCaptor = ArgumentCaptor.forClass(DescribeEventsRequest.class);
        verify(rdsProxy.client(), times(1)).describeEvents(describeEventsCaptor.capture());
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        Assertions.assertThat(describeEventsCaptor.getValue().startTime()).isEqualTo(updatedAt);
    }

    @Test
    public void handleRequest_FetchEventsThrowsAwsServiceExceptionIgnore() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(HandlerErrorCode.AccessDenied.toString())
                                .errorMessage("Access denied")
                                .build())
                        .build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setUpdated(false);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_SourceDBInstanceAutomatedBackupsArn_UpdatedToDifferentValue() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .sourceDBInstanceAutomatedBackupsArn("arn:aws:rds:us-east-1:123456789012:snapshot:rds:backup-name-new")
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .sourceDBInstanceAutomatedBackupsArn(SOURCE_DB_INSTANCE_AUTOMATED_BACKUPS_ARN_NON_EMPTY)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_SourceDbiResourceId_UpdatedFromValueToEmpty() {
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .sourceDbiResourceId(null)
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .sourceDbiResourceId(SOURCE_DBI_RESOURCE_ID_NON_EMPTY)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                () -> DB_INSTANCE_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_SourceDbiResourceId_UpdatedToDifferentValue() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .sourceDbiResourceId("dbi-instance-identifier-new")
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .sourceDbiResourceId(SOURCE_DBI_RESOURCE_ID_NON_EMPTY)
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_ObserveFailureEvent() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder()
                        .events(Event.builder()
                                .message("Database instance is in a state that cannot be upgraded: PreUpgrade checks failed: The instance could not be upgraded")
                                .build())
                        .build());

        final CallbackContext context = new CallbackContext();
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setUpdated(false);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.GeneralServiceException)
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_startAutomaticBackupReplication() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setAddTagsComplete(true);
        context.setAutomaticBackupReplicationStarted(false);
        context.setAutomaticBackupReplicationStopped(true);

        proxy = Mockito.spy(proxy);

        final RdsClient crossRegionRdsClient = mock(RdsClient.class);
        final ProxyClient<RdsClient> crossRegionRdsProxy = mockProxy(proxy, crossRegionRdsClient);
        doReturn(crossRegionRdsProxy).when(proxy).newProxy(ArgumentMatchers.<Supplier<RdsClient>>any());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE.toBuilder()
                        .dbInstanceAutomatedBackupsReplications(Collections.singletonList(DBInstanceAutomatedBackupsReplication.builder()
                                .dbInstanceAutomatedBackupsArn(getAutomaticBackupArn(AUTOMATIC_BACKUP_REPLICATION_REGION_ALTER)).build()))
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .automaticBackupReplicationRegion(AUTOMATIC_BACKUP_REPLICATION_REGION)
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .automaticBackupReplicationRegion(AUTOMATIC_BACKUP_REPLICATION_REGION_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(crossRegionRdsProxy.client(), times(1)).startDBInstanceAutomatedBackupsReplication(any(StartDbInstanceAutomatedBackupsReplicationRequest.class));
        verify(crossRegionRdsProxy.client(), atLeastOnce()).serviceName();
        verifyNoMoreInteractions(crossRegionRdsProxy.client());
        verifyAccessPermissions(crossRegionRdsProxy.client());
        verify(rdsProxy.client(), times(4)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_stopAutomaticBackupReplication() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setAddTagsComplete(true);
        context.setAutomaticBackupReplicationStarted(true);
        context.setAutomaticBackupReplicationStopped(false);

        proxy = Mockito.spy(proxy);

        final RdsClient crossRegionRdsClient = mock(RdsClient.class);
        final ProxyClient<RdsClient> crossRegionRdsProxy = mockProxy(proxy, crossRegionRdsClient);
        doReturn(crossRegionRdsProxy).when(proxy).newProxy(ArgumentMatchers.<Supplier<RdsClient>>any());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE.toBuilder()
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .automaticBackupReplicationRegion(AUTOMATIC_BACKUP_REPLICATION_REGION)
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .automaticBackupReplicationRegion(AUTOMATIC_BACKUP_REPLICATION_REGION_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(crossRegionRdsProxy.client(), times(1)).stopDBInstanceAutomatedBackupsReplication(any(StopDbInstanceAutomatedBackupsReplicationRequest.class));
        verify(crossRegionRdsProxy.client(), atLeastOnce()).serviceName();
        verifyAccessPermissions(crossRegionRdsProxy.client());
        verifyNoMoreInteractions(crossRegionRdsProxy.client());
        verify(rdsProxy.client(), times(4)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_updateStorageTypeFromIo1ToIo2() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);
        context.setAddTagsComplete(true);
        context.setAutomaticBackupReplicationStarted(true);
        context.setAutomaticBackupReplicationStopped(true);

        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE.toBuilder()
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .storageType("io1")
                        .allocatedStorage("100")
                        .iops(1000)
                        .build(),
                () -> RESOURCE_MODEL_BLDR()
                        .storageType("io2")
                        .allocatedStorage("100")
                        .iops(1000)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        final ArgumentCaptor<ModifyDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client()).modifyDBInstance(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().iops()).isEqualTo(1000);
        Assertions.assertThat(argumentCaptor.getValue().allocatedStorage()).isEqualTo(100);
        Assertions.assertThat(argumentCaptor.getValue().storageType()).isEqualTo("io2");

        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_EngineLifecycleSupportShouldFail() {
        expectServiceInvocation = false;
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .engineLifecycleSupport("open-source-rds-extended-support")
                .build();

        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .engineLifecycleSupport("open-source-rds-extended-support-disabled")
                .build();

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(false),
                null,
                () -> previousModel,
                () -> desiredModel,
                expectFailed(HandlerErrorCode.InvalidRequest)
        );
    }
}
