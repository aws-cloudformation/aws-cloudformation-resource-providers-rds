package software.amazon.rds.dbinstance;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.rds.dbinstance.BaseHandlerStd.API_VERSION_V12;

import java.time.Duration;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.util.SdkAutoConstructList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.AuthorizationNotFoundException;
import software.amazon.awssdk.services.rds.model.CertificateNotFoundException;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaResponse;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupDoesNotCoverEnoughAZsException;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RebootDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeResponse;
import software.amazon.awssdk.services.rds.model.StorageTypeNotSupportedException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.dbinstance.status.DomainMembershipStatus;
import software.amazon.rds.dbinstance.status.OptionGroupStatus;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

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
    private CreateHandler handler;

    private boolean expectServiceInvocation;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
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
        handler = new CreateHandler(HandlerConfig.builder()
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
    public void handleRequest_RestoreDBInstanceFromSnapshot_Success() {
        final RestoreDbInstanceFromDbSnapshotResponse restoreResponse = RestoreDbInstanceFromDbSnapshotResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshotV12_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxyV12.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());
        // rdsClientV12 would be invoked on a stabilization.
        when(rdsProxyV12.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder()
                        // An attempt to create a new instance with DBSecurityGroups should downgrade the client to V12
                        .dBSecurityGroups(DB_SECURITY_GROUPS)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> argumentCaptor = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxyV12.client(), times(1)).restoreDBInstanceFromDBSnapshot(argumentCaptor.capture());
        verify(rdsProxyV12.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_AccessDeniedTagging() {
        final RestoreDbInstanceFromDbSnapshotResponse restoreResponse = RestoreDbInstanceFromDbSnapshotResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(restoreResponse);
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_INSTANCE_ACTIVE,
                null,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> createCaptor = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(2)).restoreDBInstanceFromDBSnapshot(createCaptor.capture());

        final RestoreDbInstanceFromDbSnapshotRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class)
        );
        final RestoreDbInstanceFromDbSnapshotRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagCaptor.capture());
        Assertions.assertThat(addTagCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_DefaultMZ() {
        final DescribeDbSnapshotsResponse describeDbSnapshotsResponse = DescribeDbSnapshotsResponse.builder()
                .dbSnapshots(DBSnapshot.builder().engine("mysql").build())
                .build();
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenReturn(describeDbSnapshotsResponse);
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder().multiAZ(null).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> argument = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(argument.capture());
        Assertions.assertThat(argument.getValue().multiAZ()).isEqualTo(false);
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromClusterSnapshot_NoFetchingDefaultMZ() {
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder()
                        .multiAZ(null)
                        .dBSnapshotIdentifier(null)
                        .dBClusterSnapshotIdentifier("cluster-snapshot").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> argument = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(argument.capture());
        Assertions.assertThat(argument.getValue().multiAZ()).isEqualTo(null);
        Assertions.assertThat(argument.getValue().dbClusterSnapshotIdentifier()).isEqualTo("cluster-snapshot");
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_DefaultMZ_SqlServer() {
        final DescribeDbSnapshotsResponse describeDbSnapshotsResponse = DescribeDbSnapshotsResponse.builder()
                .dbSnapshots(DBSnapshot.builder().engine("sqlserver-ee").build())
                .build();
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenReturn(describeDbSnapshotsResponse);
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder().multiAZ(null).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> argument = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(argument.capture());
        Assertions.assertThat(argument.getValue().multiAZ()).isNull();
    }


    @Test
    public void handleRequest_CreateReadReplica_Create_Success() {
        when(rdsProxy.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenReturn(CreateDbInstanceReadReplicaResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_READ_REPLICA,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_AccessDeniedTagging() {
        when(rdsProxy.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateDbInstanceReadReplicaResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_INSTANCE_ACTIVE,
                null,
                () -> RESOURCE_MODEL_READ_REPLICA.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceReadReplicaRequest> createCaptor = ArgumentCaptor.forClass(CreateDbInstanceReadReplicaRequest.class);
        verify(rdsProxy.client(), times(2)).createDBInstanceReadReplica(createCaptor.capture());

        final CreateDbInstanceReadReplicaRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class)
        );
        final CreateDbInstanceReadReplicaRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagCaptor.capture());
        Assertions.assertThat(addTagCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_CreateReadReplica_Update_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_READ_REPLICA,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_Reboot_Success() {
        when(rdsProxy.client().rebootDBInstance(any(RebootDbInstanceRequest.class)))
                .thenReturn(RebootDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_READ_REPLICA,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_Success() {
        when(rdsProxy.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenReturn(CreateDbInstanceReadReplicaResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_READ_REPLICA,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_AccessDeniedException() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(2)).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstanceV12_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxyV12.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenReturn(CreateDbInstanceResponse.builder().build());
        // rdsClientV12 would be invoked on a stabilization.
        when(rdsProxyV12.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        // An attempt to create a new instance with DBSecurityGroups should downgrade the client to V12
                        .dBSecurityGroups(DB_SECURITY_GROUPS)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceRequest> argumentCaptor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxyV12.client(), times(1)).createDBInstance(argumentCaptor.capture());
        verify(rdsProxyV12.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));

        Assertions.assertThat(argumentCaptor.getValue().dbSecurityGroups()).containsExactly(Iterables.toArray(DB_SECURITY_GROUPS, String.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_AccessDeniedTagging() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_INSTANCE_ACTIVE,
                null,
                () -> RESOURCE_MODEL_BLDR()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceRequest> createCaptor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxy.client(), times(2)).createDBInstance(createCaptor.capture());

        final CreateDbInstanceRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class)
        );
        final CreateDbInstanceRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagCaptor.capture());
        Assertions.assertThat(addTagCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_CreateDBInstance_SoftFailTagsReentrance() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenReturn(CreateDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.getTaggingContext().setSoftFailTags(true);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_INSTANCE_ACTIVE,
                null,
                () -> RESOURCE_MODEL_BLDR()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceRequest> createCaptor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstance(createCaptor.capture());

        final CreateDbInstanceRequest requestWithSystemTags = createCaptor.getValue();
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagCaptor.capture());
        Assertions.assertThat(addTagCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_CreateNewInstance_UpdateRoles_Success() {
        when(rdsProxy.client().addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class)))
                .thenReturn(AddRoleToDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final Queue<DBInstance> transitions = new ConcurrentLinkedQueue<>(
                computeAssociatedRoleTransitions(DB_INSTANCE_ACTIVE, Collections.emptyList(), ASSOCIATED_ROLES)
        );
        transitions.add(DB_INSTANCE_ACTIVE.toBuilder()
                .associatedRoles(Translator.translateAssociatedRolesToSdk(ASSOCIATED_ROLES))
                .build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(false);

        test_handleRequest_base(
                context,
                transitions::remove,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(5)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(3)).addRoleToDBInstance(any(AddRoleToDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_NoIdentifier_Success() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenReturn(CreateDbInstanceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_NO_IDENTIFIER,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBInstance(any(CreateDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldNotUpdateAfterCreate_CACertificateIdentifier_Success() {
        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .caCertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(false);

        test_handleRequest_base(
                context,
                () -> dbInstance,
                () -> Translator.translateDbInstanceFromSdk(dbInstance),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceRequest> createCaptor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstance(createCaptor.capture());

        final CreateDbInstanceRequest requestWithCertificate = createCaptor.getValue();
        Assertions.assertThat(requestWithCertificate.caCertificateIdentifier()).isEqualTo(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY);
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreFromSnapshot_ShouldUpdateAfterCreate_AllocatedStorage_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final DBInstance dbInstance = DB_INSTANCE_BASE.toBuilder()
                .engine(ENGINE_SQLSERVER_SE)
                .allocatedStorage(100)
                .iops(3000)
                .build();
        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);

        test_handleRequest_base(
                context,
                () -> dbInstance,
                () -> Translator.translateDbInstanceFromSdk(dbInstance).toBuilder().dBSnapshotIdentifier("snapshot").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldNotReboot_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().dBParameterGroupName(null).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateNewInstance_ShouldNotUpdate_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().cACertificateIdentifier(null).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_DbSecurityGroups_ShouldUpdate_Success() {
        when(rdsProxyV12.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());
        when(rdsProxyV12.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .dBSecurityGroups(Collections.singletonList(DB_SECURITY_GROUP_DEFAULT))
                        .build(),
                expectSuccess()
        );

        verify(rdsProxyV12.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxyV12.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_AllocatedStorage_ShouldNotUpdate_Success() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        when(rdsProxy.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenReturn(CreateDbInstanceReadReplicaResponse.builder().build());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .engine(ENGINE_MYSQL)
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .allocatedStorage(ALLOCATED_STORAGE.toString())
                        .build(),
                expectSuccess()
        );


        ArgumentCaptor<CreateDbInstanceReadReplicaRequest> captor = ArgumentCaptor.forClass(CreateDbInstanceReadReplicaRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstanceReadReplica(captor.capture());
        Assertions.assertThat(captor.getValue().allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_AllocatedStorage_ShouldNotUpdate_Success() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenReturn(DescribeDbSnapshotsResponse.builder()
                        .dbSnapshots(ImmutableList.of(DBSnapshot.builder().build())).build());

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .engine(ENGINE_MYSQL)
                        .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER_NON_EMPTY)
                        .allocatedStorage(ALLOCATED_STORAGE.toString())
                        .build(),
                expectSuccess()
        );


        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(captor.capture());
        Assertions.assertThat(captor.getValue().allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_CACertificateIdentifier_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .cACertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_DBParameterGroup_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_EngineVersion_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .engineVersion(ENGINE_VERSION_MYSQL_56)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_MasterUserPassword_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .masterUserPassword(MASTER_USER_PASSWORD)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_PreferredBackupWindow_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_NON_EMPTY)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_PreferredMaintenanceWindow_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .preferredMaintenanceWindow(PREFERRED_BACKUP_WINDOW_NON_EMPTY)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_BackupRetentionPeriod_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_Iops_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_SQLSERVER_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .iops(IOPS_DEFAULT)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_MaxAllocatedStorage_ShouldUpdate_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_SQLSERVER_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // Read replica
                        .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateClusterDBInstance_SnapshotRequested() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                null,
                null,
                () -> RESOURCE_MODEL_BLDR()
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .build(),
                expectFailed(HandlerErrorCode.InvalidRequest)
        );
    }

    @Test
    public void handleRequest_CreateDBInstance_EmptyPortString() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .port("")
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbInstanceRequest> argument = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstance(argument.capture());
        Assertions.assertThat(argument.getValue().port()).isNull();
    }

    @Test
    public void handleRequest_CreateDBInstance_DbClusterNotFoundException() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(DbClusterNotFoundException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorMessage("The source cluster could not be found")
                                .build()
                        ).build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_NON_EMPTY)
                        .build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateDBInstance_OptionGroupNotFoundException() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenThrow(OptionGroupNotFoundException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorMessage("Specified OptionGroupName not found")
                                .build()
                        ).build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                        .build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateDBInstance_OptionGroupInTerminalState() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);

        Assertions.assertThatThrownBy(() -> {
            test_handleRequest_base(
                    context,
                    () -> DB_INSTANCE_ACTIVE.toBuilder()
                            .optionGroupMemberships(OptionGroupMembership.builder()
                                    .status(OptionGroupStatus.Failed.toString())
                                    .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                                    .build()).build(),
                    () -> RESOURCE_MODEL_BAREBONE_BLDR().build(),
                    expectFailed(HandlerErrorCode.NotStabilized)
            );
        }).isInstanceOf(CfnNotStabilizedException.class);

        verify(rdsProxy.client(), times(1)).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_CreateDBInstance_DomainMembershipInTerminalState() {
        final CallbackContext context = new CallbackContext();
        context.setCreated(false);

        Assertions.assertThatThrownBy(() -> {
            test_handleRequest_base(
                    context,
                    () -> DB_INSTANCE_ACTIVE.toBuilder()
                            .domainMemberships(DomainMembership.builder()
                                    .status(DomainMembershipStatus.Failed.toString())
                                    .domain("domain")
                                    .build()).build(),
                    () -> RESOURCE_MODEL_BAREBONE_BLDR().build(),
                    expectFailed(HandlerErrorCode.NotStabilized)
            );
        }).isInstanceOf(CfnNotStabilizedException.class);

        verify(rdsProxy.client(), times(1)).createDBInstance(any(CreateDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_RestoreFromSnapshot_EmptyVpcSecurityGroupIdList() {
        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder()
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(captor.capture());
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isEmpty();
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isInstanceOf(SdkAutoConstructList.class);
    }

    @Test
    public void handleRequest_CreateDBInstance_EmptyVpcSecurityGroupIdList() {
        when(rdsProxy.client().createDBInstance(any(CreateDbInstanceRequest.class)))
                .thenReturn(CreateDbInstanceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<CreateDbInstanceRequest> captor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstance(captor.capture());
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isEmpty();
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isInstanceOf(SdkAutoConstructList.class);
    }

    @Test
    public void handleRequest_CreateDBInstance_EmptyVpcSecurityGroupIdListInUpdate() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setAddTagsComplete(true);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR()
                        .vPCSecurityGroups(Collections.emptyList())
                        .dBClusterSnapshotIdentifier("identifier")
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client()).modifyDBInstance(captor.capture());
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isEmpty();
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isInstanceOf(SdkAutoConstructList.class);
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateReadReplica_EmptyVpcSecurityGroupIdList() {
        when(rdsProxy.client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)))
                .thenReturn(CreateDbInstanceReadReplicaResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_READ_REPLICA.toBuilder()
                        .vPCSecurityGroups(Collections.emptyList())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<CreateDbInstanceReadReplicaRequest> captor = ArgumentCaptor.forClass(CreateDbInstanceReadReplicaRequest.class);
        verify(rdsProxy.client(), times(1)).createDBInstanceReadReplica(captor.capture());
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isEmpty();
        Assertions.assertThat(captor.getValue().vpcSecurityGroupIds()).isInstanceOf(SdkAutoConstructList.class);
    }

    @Test
    public void handleRequest_RestoreDBInstanceFromSnapshot_UpdateEngineVersion() {
        final String snapshotEngineVersion = ENGINE_VERSION_MYSQL_56;
        final String requestedEngineVersion = ENGINE_VERSION_MYSQL_80;

        when(rdsProxy.client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)))
                .thenReturn(RestoreDbInstanceFromDbSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE.toBuilder()
                        .engineVersion(snapshotEngineVersion)
                        .build(),
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT.toBuilder()
                        .engineVersion(requestedEngineVersion)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class));

        ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBInstance(captor.capture());
        Assertions.assertThat(captor.getValue().engineVersion()).isEqualTo(requestedEngineVersion);
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    static class CreateDBInstanceExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.DBClusterNotFoundFault, HandlerErrorCode.NotFound),
                    Arguments.of(ErrorCode.DBSubnetGroupNotAllowedFault, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.InvalidParameterCombination, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.StorageTypeNotSupportedFault, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.ThrottlingException, HandlerErrorCode.Throttling),
                    // Put exception classes below
                    Arguments.of(AuthorizationNotFoundException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.InvalidRequest),
                    Arguments.of(CertificateNotFoundException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.NotFound),
                    Arguments.of(DbClusterSnapshotNotFoundException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.NotFound),
                    Arguments.of(DbInstanceAlreadyExistsException.builder().message(MSG_ALREADY_EXISTS_ERR).build(), HandlerErrorCode.AlreadyExists),
                    Arguments.of(DbSubnetGroupDoesNotCoverEnoughAZsException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.InvalidRequest),
                    Arguments.of(DomainNotFoundException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.NotFound),
                    Arguments.of(InvalidSubnetException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.GeneralServiceException),
                    Arguments.of(new RuntimeException(MSG_GENERIC_ERR), HandlerErrorCode.InternalFailure),
                    Arguments.of(StorageTypeNotSupportedException.builder().message(MSG_GENERIC_ERR).build(), HandlerErrorCode.InvalidRequest)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CreateDBInstanceExceptionArgumentsProvider.class)
    public void handleRequest_CreateDBInstance_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectCreateDBInstanceCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_BLDR().build(),
                requestException,
                expectResponseCode
        );
    }

    static class CreateDBInstanceReadReplicaExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    // <empty>
                    // Put exception classes below
                    Arguments.of(DbInstanceAlreadyExistsException.builder().message(MSG_ALREADY_EXISTS_ERR).build(), HandlerErrorCode.AlreadyExists),
                    Arguments.of(new RuntimeException(MSG_GENERIC_ERR), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CreateDBInstanceReadReplicaExceptionArgumentsProvider.class)
    public void handleRequest_CreateDBInstanceReadReplica_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectCreateDBInstanceReadReplicaCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_READ_REPLICA,
                requestException,
                expectResponseCode
        );
    }

    static class RestoreDBInstanceFromSnapshotExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.InvalidDBSnapshotState, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.InvalidRestoreFault, HandlerErrorCode.InvalidRequest),
                    // Put exception classes below
                    Arguments.of(DbInstanceAlreadyExistsException.builder().message(MSG_ALREADY_EXISTS_ERR).build(), HandlerErrorCode.AlreadyExists),
                    Arguments.of(new RuntimeException(MSG_GENERIC_ERR), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RestoreDBInstanceFromSnapshotExceptionArgumentsProvider.class)
    public void handleRequest_RestoreDBInstanceFromSnapshot_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectRestoreDBInstanceFromDBSnapshotCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT,
                requestException,
                expectResponseCode
        );
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_TriggeredBy_UseLatestRestorableTime() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .useLatestRestorableTime(USE_LATEST_RESTORABLE_TIME_YES)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_TriggeredBy_SourceDbiResourceId() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .sourceDbiResourceId(SOURCE_DBI_RESOURCE_ID_NON_EMPTY)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_NullIdentifiers() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .useLatestRestorableTime(USE_LATEST_RESTORABLE_TIME_YES)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceToPointInTimeRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceToPointInTimeRequest.class);

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        // Both identifiers are passed as null. DBInstanceIdentifier will be random and the same value will be used for TargetDBInstanceIdentifier
        // All of these InstanceIdentifier tests are non-perfect since we can't tell the value of DBInstanceIdentifier. So this is only half the picture
        Assertions.assertThat(captor.getValue().targetDBInstanceIdentifier()).isNotBlank();
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_SpecificDBInstanceIdentifier() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                        .useLatestRestorableTime(USE_LATEST_RESTORABLE_TIME_YES)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceToPointInTimeRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceToPointInTimeRequest.class);

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        // Specific DBInstanceIdentifier. The same value will be used for TargetDBInstanceIdentifier
        Assertions.assertThat(captor.getValue().targetDBInstanceIdentifier()).isEqualTo(DB_INSTANCE_IDENTIFIER_NON_EMPTY);
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_TriggeredBy_RestoreTimeUTC() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .restoreTime(RESTORE_TIME_UTC)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceToPointInTimeRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceToPointInTimeRequest.class);

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        Assertions.assertThat(captor.getValue().restoreTime()).isEqualTo(RESTORE_TIME_UTC);
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_TriggeredBy_RestoreTimeUTCPlus5() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenReturn(restoreResponse);

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .restoreTime(RESTORE_TIME_UTC_PLUS_5)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceToPointInTimeRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceToPointInTimeRequest.class);

        verify(rdsProxy.client(), times(1)).restoreDBInstanceToPointInTime(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        Assertions.assertThat(captor.getValue().restoreTime()).isEqualTo(RESTORE_TIME_UTC);
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_AccessDeniedTagging() {
        final RestoreDbInstanceToPointInTimeResponse restoreResponse = RestoreDbInstanceToPointInTimeResponse.builder().build();
        when(rdsProxy.client().restoreDBInstanceToPointInTime(any(RestoreDbInstanceToPointInTimeRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(restoreResponse);
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(false);
        context.setUpdated(true);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_INSTANCE_ACTIVE,
                null,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .useLatestRestorableTime(true)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbInstanceToPointInTimeRequest> createCaptor = ArgumentCaptor.forClass(RestoreDbInstanceToPointInTimeRequest.class);
        verify(rdsProxy.client(), times(2)).restoreDBInstanceToPointInTime(createCaptor.capture());

        final RestoreDbInstanceToPointInTimeRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class)
        );
        final RestoreDbInstanceToPointInTimeRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagCaptor.capture());
        Assertions.assertThat(addTagCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_Update_Success() {
        when(rdsProxy.client().modifyDBInstance(any(ModifyDbInstanceRequest.class)))
                .thenReturn(ModifyDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(false);
        context.setRebooted(true);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .useLatestRestorableTime(true)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBInstance(any(ModifyDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_RestoreDBInstanceToPointInTime_Reboot_Success() {
        when(rdsProxy.client().rebootDBInstance(any(RebootDbInstanceRequest.class)))
                .thenReturn(RebootDbInstanceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setCreated(true);
        context.setUpdated(true);
        context.setRebooted(false);
        context.setUpdatedRoles(true);

        test_handleRequest_base(
                context,
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_RESTORING_TO_POINT_IN_TIME.toBuilder()
                        .useLatestRestorableTime(true)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).rebootDBInstance(any(RebootDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }
}
