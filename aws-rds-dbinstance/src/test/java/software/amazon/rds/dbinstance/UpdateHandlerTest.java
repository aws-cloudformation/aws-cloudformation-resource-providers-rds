package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    RdsClient rdsClient;

    @Mock
    Ec2Client ec2Client;

    @Mock
    private ProxyClient<RdsClient> rdsProxyClient;

    @Mock
    private ProxyClient<Ec2Client> ec2ProxyClient;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);
        rdsProxyClient = MOCK_PROXY(proxy, rdsClient);
        ec2ProxyClient = MOCK_PROXY(proxy, ec2Client);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
    }

    @Test
    public void handleRequest_InitiatesModifyRequest() {
        final ModifyDbInstanceResponse modifyDbInstanceResponse = ModifyDbInstanceResponse.builder()
                .dbInstance(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyDbInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_BLDR().build())
                .desiredResourceState(RESOURCE_MODEL_ALTER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessTagsAddOnly() {
        final DescribeDbInstancesResponse describeDbInstancesResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ResourceModel resourceModel = RESOURCE_MODEL_BLDR().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                .previousResourceState(resourceModel)
                .previousResourceTags(Collections.emptyMap())
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessTagsRemoveOnly() {
        final DescribeDbInstancesResponse describeDbInstancesResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rdsProxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final ResourceModel resourceModel = RESOURCE_MODEL_BLDR().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .previousResourceState(resourceModel)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_EMPTY))
                .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final CallbackContext context = new CallbackContext();
        context.setModified(true); // this is an emulation of a re-entrance

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        test_handleRequest_base(
                context,
                RESOURCE_MODEL_BLDR().build(),
                RESOURCE_MODEL_BLDR().build(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_INSTANCE_MODIFYING;
                    }
                    return DB_INSTANCE_ACTIVE;
                },
                ResourceHandlerRequest.<ResourceModel>builder()
        );

        verify(rdsProxyClient.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
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

        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenThrow(DbInstanceRoleNotFoundException.class);
        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                RESOURCE_MODEL_BLDR().build(),
                RESOURCE_MODEL_ALTER,
                transitions::remove,
                ResourceHandlerRequest.<ResourceModel>builder()
        );

        verify(rdsProxyClient.client()).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxyClient.client(), times(2)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
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
        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenThrow(DbInstanceRoleAlreadyExistsException.class);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                RESOURCE_MODEL_BLDR().build(),
                RESOURCE_MODEL_ALTER,
                transitions::remove,
                ResourceHandlerRequest.<ResourceModel>builder()
        );

        verify(rdsProxyClient.client()).removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class));
        verify(rdsProxyClient.client()).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_UpdateRoles_InternalExceptionOnAdd() {
        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).then(res -> {
            throw new RuntimeException("planned exception");
        });
        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);
        final DescribeDbInstancesResponse describeDbInstancesResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_BLDR().build())
                .desiredResourceState(RESOURCE_MODEL_ALTER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    public void handleRequest_UpdateRolesInternalExceptionOnRemove() {
        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).then(res -> {
            throw new RuntimeException("expected exception");
        });
        final DescribeDbInstancesResponse describeDbInstancesResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_BLDR().build())
                .desiredResourceState(RESOURCE_MODEL_ALTER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    public void handleRequest_UpdateRolesNotFoundOnTagging() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_INSTANCE_ACTIVE);
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            if (!transitions.isEmpty()) {
                return DescribeDbInstancesResponse.builder()
                        .dbInstances(transitions.remove())
                        .build();
            }
            throw DbInstanceNotFoundException.builder().build();
        });

        final ResourceModel resourceModel = RESOURCE_MODEL_BLDR().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(resourceModel)
                .desiredResourceState(resourceModel)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void handleRequest_UpdateRolesInternalExceptionOnTagging() {
        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, ASSOCIATED_ROLES, ASSOCIATED_ROLES_ALTER)
        );
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            if (!transitions.isEmpty()) {
                return DescribeDbInstancesResponse.builder()
                        .dbInstances(transitions.remove())
                        .build();
            }
            throw new RuntimeException("expected exception");
        });
        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);
        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_BLDR().build())
                .desiredResourceState(RESOURCE_MODEL_ALTER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
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

        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            return DescribeDbInstancesResponse.builder()
                    .dbInstances(transitions.remove())
                    .build();
        });
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rdsProxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsProxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);
        final AddRoleToDbInstanceResponse addRoleToDBInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDBInstanceResponse);
        final RemoveRoleFromDbInstanceResponse removeRoleFromDBInstanceResponse = RemoveRoleFromDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().removeRoleFromDBInstance(any(RemoveRoleFromDbInstanceRequest.class))).thenReturn(removeRoleFromDBInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_BLDR().build())
                .desiredResourceState(RESOURCE_MODEL_ALTER)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER))
                .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ShouldReboot() {
        final DBInstance dbInstancePendingReboot = DB_INSTANCE_ACTIVE.toBuilder().dbParameterGroups(
                ImmutableList.of(DBParameterGroupStatus.builder()
                        .dbParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                        .parameterApplyStatus(UpdateHandler.PENDING_REBOOT_STATUS)
                        .build())
        ).build();

        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);

        final CallbackContext context = new CallbackContext();
        context.setModified(true); // this is an emulation of a re-entrance
        context.setRolesUpdated(true);

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);

        test_handleRequest_base(
                context,
                RESOURCE_MODEL_BLDR().build(),
                RESOURCE_MODEL_BLDR().build(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_INSTANCE_MODIFYING;
                    }
                    return dbInstancePendingReboot;
                },
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true)
        );

        verify(rdsProxyClient.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_SetParameterGroupName() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of(DBParameterGroup.builder().build()))
                .build();
        when(rdsProxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final DescribeDbEngineVersionsResponse describeDbEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(DBEngineVersion.builder().build())
                .build();
        when(rdsProxyClient.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class))).thenReturn(describeDbEngineVersionsResponse);

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
        context.setModified(true); // this is an emulation of a re-entrance

        test_handleRequest_base(
                context,
                previousModel,
                desiredModel,
                () -> DB_INSTANCE_ACTIVE,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true)
        );

        verify(rdsProxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(rdsProxyClient.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
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
        context.setModified(true); // this is an emulation of a re-entrance

        test_handleRequest_base(
                context,
                previousModel,
                desiredModel,
                () -> DB_INSTANCE_ACTIVE,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true)
        );
    }

    @Test
    public void handleRequest_SetParameterGroupName_NoDbParameterGroups() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of()) // empty db parameter group set
                .build();
        when(rdsProxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

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
        context.setModified(true); // this is an emulation of a re-entrance

        test_handleRequest_base(
                context,
                previousModel,
                desiredModel,
                () -> DB_INSTANCE_ACTIVE,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true)
        );

        verify(rdsProxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
    }

    @Test
    public void handleRequest_SetParameterGroupName_EmptyDbEngineVersions() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(ImmutableList.of(DBParameterGroup.builder().build()))
                .build();
        when(rdsProxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final DescribeDbEngineVersionsResponse describeDbEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(ImmutableList.of()) // empty list
                .build();
        when(rdsProxyClient.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class))).thenReturn(describeDbEngineVersionsResponse);

        final CallbackContext context = new CallbackContext();
        context.setModified(true); // this is an emulation of a re-entrance

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
                previousModel,
                desiredModel,
                () -> DB_INSTANCE_ACTIVE,
                ResourceHandlerRequest.<ResourceModel>builder().rollback(true)
        );

        verify(rdsProxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(rdsProxyClient.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
    }

    @Test
    public void handleRequest_SetDefaultVpcId() {
        final DescribeSecurityGroupsResponse describeSecurityGroupsResponse = DescribeSecurityGroupsResponse.builder()
                .securityGroups(SecurityGroup.builder().groupName(DB_SECURITY_GROUP_DEFAULT).build())
                .build();
        when(ec2ProxyClient.client().describeSecurityGroups(any(DescribeSecurityGroupsRequest.class))).thenReturn(describeSecurityGroupsResponse);

        final CallbackContext context = new CallbackContext();
        context.setModified(true); // this is an emulation of a re-entrance

        test_handleRequest_base(
                context,
                RESOURCE_MODEL_BLDR().build(),
                RESOURCE_MODEL_BLDR()
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                () -> DB_INSTANCE_ACTIVE.toBuilder().dbSubnetGroup(
                        DBSubnetGroup.builder().vpcId(DB_SECURITY_GROUP_VPC_ID).build()
                ).build(),
                ResourceHandlerRequest.<ResourceModel>builder()
        );

        verify(rdsProxyClient.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(ec2ProxyClient.client()).describeSecurityGroups(any(DescribeSecurityGroupsRequest.class));
    }

    private void test_handleRequest_base(
            final CallbackContext context,
            final ResourceModel previousModel,
            final ResourceModel desiredModel,
            final Supplier<DBInstance> dbInstanceSupplier,
            final ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder
    ) {
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            return DescribeDbInstancesResponse.builder()
                    .dbInstances(dbInstanceSupplier.get())
                    .build();
        });

        final ResourceHandlerRequest<ResourceModel> request = requestBuilder
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
