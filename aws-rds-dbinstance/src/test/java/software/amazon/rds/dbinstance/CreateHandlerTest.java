package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
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
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaResponse;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership;
import software.amazon.awssdk.services.rds.model.DbInstanceAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> rdsProxyClient;

    @Mock
    private ProxyClient<Ec2Client> ec2ProxyClient;

    @Mock
    RdsClient rdsClient;

    @Mock
    Ec2Client ec2Client;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
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

    private void test_handleRequest_base(
            final CallbackContext callbackContext,
            final Supplier<DBInstance> dbInstanceSupplier,
            final Supplier<ResourceModel> resourceModelSupplier
    ) {
        final CreateDbInstanceResponse createResponse = CreateDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().createDBInstance(any(CreateDbInstanceRequest.class))).thenReturn(createResponse);

        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            return DescribeDbInstancesResponse.builder().dbInstances(dbInstanceSupplier.get()).build();
        });

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModelSupplier.get())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_Success() {
        final RestoreDbInstanceFromDbSnapshotResponse restoreResponse = RestoreDbInstanceFromDbSnapshotResponse.builder().build();
        when(rdsProxyClient.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(restoreResponse);

        final DescribeDbInstancesResponse describeResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setRolesUpdated(true);
        callbackContext.setUpdatedAfterCreate(true);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class));
        verify(rdsProxyClient.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_AlreadyExistsIgnore() {
        when(rdsProxyClient.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenThrow(DbInstanceAlreadyExistsException.builder().build());

        final DescribeDbInstancesResponse describeResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setRolesUpdated(true);
        callbackContext.setUpdatedAfterCreate(true);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class));
        verify(rdsProxyClient.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_RuntimeException() {
        when(rdsProxyClient.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenThrow(new RuntimeException());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    public void handleRequest_CreateReadReplica_Success() {
        final CreateDbInstanceReadReplicaResponse createResponse = CreateDbInstanceReadReplicaResponse.builder().build();
        when(rdsProxyClient.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class))).thenReturn(createResponse);

        final DescribeDbInstancesResponse describeResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeResponse);
        final ModifyDbInstanceResponse modifyDbInstanceResponse = ModifyDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyDbInstanceResponse);
        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_READ_REPLICA)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setRolesUpdated(true);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class));
        verify(rdsProxyClient.client()).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxyClient.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxyClient.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_AlreadyExistsIgnore() {
        when(rdsProxyClient.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenThrow(DbInstanceAlreadyExistsException.builder().build());

        final DescribeDbInstancesResponse describeResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeResponse);
        final ModifyDbInstanceResponse modifyDbInstanceResponse = ModifyDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyDbInstanceResponse);
        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_READ_REPLICA)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setRolesUpdated(true);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class));
        verify(rdsProxyClient.client()).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxyClient.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxyClient.client(), times(4)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_RuntimeException() {
        when(rdsProxyClient.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenThrow(new RuntimeException());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_READ_REPLICA)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    public void handleRequest_CreateNewInstance_AlreadyExistsIgnore() {
        when(rdsProxyClient.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(DbInstanceAlreadyExistsException.builder().build());
        final DescribeDbInstancesResponse describeResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_ACTIVE)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeResponse);
        final ModifyDbInstanceResponse modifyDbInstanceResponse = ModifyDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyDbInstanceResponse);
        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setRolesUpdated(true); // disable role update

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).createDBInstance(any(CreateDbInstanceRequest.class));
        verify(rdsProxyClient.client(), times(4)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxyClient.client()).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxyClient.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_RuntimeException() {
        when(rdsProxyClient.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(new RuntimeException());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    public void handleRequest_CreateNewInstance_Success() {
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(false);
        final ModifyDbInstanceResponse modifyResponse = ModifyDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().modifyDBInstance(any(ModifyDbInstanceRequest.class))).thenReturn(modifyResponse);
        final RebootDbInstanceResponse rebootDbInstanceResponse = RebootDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().rebootDBInstance(any(RebootDbInstanceRequest.class))).thenReturn(rebootDbInstanceResponse);
        test_handleRequest_base(context, () -> DB_INSTANCE_ACTIVE, () -> RESOURCE_MODEL_BLDR().build());

        verify(rdsProxyClient.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxyClient.client()).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxyClient.client()).modifyDBInstance(any(ModifyDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_EmptyPortSuccess() {
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> DB_INSTANCE_EMPTY_PORT, () -> RESOURCE_MODEL_BLDR().build());
        verify(rdsProxyClient.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_UpdateRolesSuccess() {
        final AddRoleToDbInstanceResponse addRoleToDbInstanceResponse = AddRoleToDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class))).thenReturn(addRoleToDbInstanceResponse);

        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, Collections.emptyList(), ASSOCIATED_ROLES)
        );
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES))
                .build());

        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(false);
        context.setUpdatedAfterCreate(true);

        test_handleRequest_base(context, transitions::remove, () -> RESOURCE_MODEL_BLDR().build());
        verify(rdsProxyClient.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxyClient.client(), times(1)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_NoIdentifierSuccess() {
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true); // disable role update
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> DB_INSTANCE_ACTIVE, () -> RESOURCE_MODEL_NO_IDENTIFIER);
        verify(rdsProxyClient.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_CACertificateIdentifier() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .caCertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_DbParameterGroupName() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .dbParameterGroups(ImmutableList.of(
                        DBParameterGroupStatus.builder().dbParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT).build()
                ))
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_EngineVersion() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_MasterUserPassword() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder().build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdkBuilder(dbInstance)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .build());
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_PreferredBackupWindow() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_NON_EMPTY)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_PreferredMaintenanceWindow() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_NON_EMPTY)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_Iops() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .iops(IOPS_DEFAULT)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_MaxAllocatedStorage() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_AllocatedStorage() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .allocatedStorage(ALLOCATED_STORAGE)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_BackupRetentionPeriod() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldUpdateAfterCreate_DbSecurityGroups() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .dbSecurityGroups(ImmutableList.of(DBSecurityGroupMembership.builder().dbSecurityGroupName(DB_SECURITY_GROUP_DEFAULT).build()))
                .build();
        final CallbackContext context = new CallbackContext();
        context.setRolesUpdated(true);
        context.setUpdatedAfterCreate(true);
        test_handleRequest_base(context, () -> dbInstance, () -> Translator.translateDbInstanceFromSdk(dbInstance));
    }
}
