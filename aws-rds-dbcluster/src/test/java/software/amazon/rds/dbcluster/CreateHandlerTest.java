package software.amazon.rds.dbcluster;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeResponse;
import software.amazon.awssdk.services.rds.model.ServerlessV2ScalingConfiguration;
import software.amazon.awssdk.services.rds.model.StorageTypeNotAvailableException;
import software.amazon.awssdk.services.rds.model.StorageTypeNotSupportedException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    RdsClient rdsClient;

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
    private CreateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(
                HandlerConfig.builder()
                        .backoff(Constant.of()
                                .delay(Duration.ofSeconds(1))
                                .timeout(Duration.ofSeconds(120))
                                .build())
                        .build()
        );
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_CreateDbCluster_Success() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder()
                        .associatedRoles(ImmutableList.of(ROLE))
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_CreateDbCluster_ServerlessV2ScalingConfiguration() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder()
                        .serverlessV2ScalingConfiguration(SERVERLESS_V2_SCALING_CONFIGURATION)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<CreateDbClusterRequest> captor = ArgumentCaptor.forClass(CreateDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).createDBCluster(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isNotNull();
        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isEqualTo(
                ServerlessV2ScalingConfiguration.builder()
                        .maxCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMaxCapacity())
                        .minCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMinCapacity())
                        .build()
        );
    }

    @Test
    public void handleRequest_CreateDbCluster_AccessDeniedTagging() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DBCLUSTER_ACTIVE,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .associatedRoles(ImmutableList.of(ROLE))
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbClusterRequest> createCaptor = ArgumentCaptor.forClass(CreateDbClusterRequest.class);
        verify(rdsProxy.client(), times(2)).createDBCluster(createCaptor.capture());
        final CreateDbClusterRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final CreateDbClusterRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }

    @Test
    public void handleRequest_CreateDbCluster_TestStabilisation() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_INPROGRESS);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL.toBuilder()
                        .associatedRoles(ImmutableList.of(ROLE))
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_CreateDbCluster_AssociatedRoleWithEmptyFeatureNameShouldStabilize() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        final Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_INPROGRESS);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE.toBuilder()
                            .associatedRoles(ImmutableList.of(
                                    software.amazon.awssdk.services.rds.model.DBClusterRole.builder()
                                            .roleArn(ROLE_ARN)
                                            .build()
                            ))
                            .build();
                },
                () -> RESOURCE_MODEL.toBuilder()
                        .associatedRoles(ImmutableList.of(
                                DBClusterRole.builder()
                                        .roleArn(ROLE_ARN)
                                        .featureName("")
                                        .build()
                        ))
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_UnsetPort() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE.toBuilder()
                        .engineMode(EngineMode.Serverless.toString())
                        .engine("aurora-mysql")
                        .port(null)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterFromSnapshotRequest> restoreCaptor = ArgumentCaptor.forClass(RestoreDbClusterFromSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(restoreCaptor.capture());
        final ArgumentCaptor<ModifyDbClusterRequest> modifyCaptor = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(modifyCaptor.capture());
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));

        // We expect the default engine-specific port to be set
        Assertions.assertThat(restoreCaptor.getValue().port()).isNotNull();
        // Modify request should have no port as it is the same as the default
        Assertions.assertThat(modifyCaptor.getValue().port()).isNull();
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_ModifyAfterCreate() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_AccessDeniedTagging() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DBCLUSTER_ACTIVE,
                null,
                () -> RESOURCE_MODEL_ON_RESTORE.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbClusterFromSnapshotRequest> createCaptor = ArgumentCaptor.forClass(RestoreDbClusterFromSnapshotRequest.class);
        verify(rdsProxy.client(), times(2)).restoreDBClusterFromSnapshot(createCaptor.capture());
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));

        final RestoreDbClusterFromSnapshotRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final RestoreDbClusterFromSnapshotRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_Success() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_ServerlessV2ScalingConfiguration() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE.toBuilder()
                        .serverlessV2ScalingConfiguration(SERVERLESS_V2_SCALING_CONFIGURATION)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterFromSnapshotRequest> captor = ArgumentCaptor.forClass(RestoreDbClusterFromSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(captor.capture());
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isNotNull();
        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isEqualTo(
                ServerlessV2ScalingConfiguration.builder()
                        .maxCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMaxCapacity())
                        .minCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMinCapacity())
                        .build()
        );

        final ArgumentCaptor<ModifyDbClusterRequest> modifyCaptor = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(modifyCaptor.capture());
        Assertions.assertThat(modifyCaptor.getValue().serverlessV2ScalingConfiguration()).isNull();
    }

    @Test
    public void handleRequest_RestoreDbClusterFromSnapshot_SetKmsKeyId() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        final String kmsKeyId = TestUtils.randomString(32, TestUtils.ALPHA);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE.toBuilder().kmsKeyId(kmsKeyId).build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterFromSnapshotRequest> argumentCaptor = ArgumentCaptor.forClass(RestoreDbClusterFromSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(argumentCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(argumentCaptor.getValue().kmsKeyId()).isEqualTo(kmsKeyId);
    }

    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_Success() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_UpdateVpcSecurityGroups() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());
        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME.toBuilder()
                        .vpcSecurityGroupIds(VPC_SG_IDS)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterToPointInTimeRequest> argumentCaptor = ArgumentCaptor.forClass(RestoreDbClusterToPointInTimeRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().vpcSecurityGroupIds()).isEqualTo(VPC_SG_IDS);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_ServerlessV2ScalingConfiguration() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME.toBuilder()
                        .serverlessV2ScalingConfiguration(SERVERLESS_V2_SCALING_CONFIGURATION)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterToPointInTimeRequest> captor = ArgumentCaptor.forClass(RestoreDbClusterToPointInTimeRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(captor.capture());
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isNotNull();
        Assertions.assertThat(captor.getValue().serverlessV2ScalingConfiguration()).isEqualTo(
                ServerlessV2ScalingConfiguration.builder()
                        .maxCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMaxCapacity())
                        .minCapacity(SERVERLESS_V2_SCALING_CONFIGURATION.getMinCapacity())
                        .build()
        );

        final ArgumentCaptor<ModifyDbClusterRequest> modifyCaptor = ArgumentCaptor.forClass(ModifyDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBCluster(modifyCaptor.capture());
        Assertions.assertThat(modifyCaptor.getValue().serverlessV2ScalingConfiguration()).isNull();
    }

    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_AccessDeniedTagging() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DBCLUSTER_ACTIVE,
                null,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<RestoreDbClusterToPointInTimeRequest> createCaptor = ArgumentCaptor.forClass(RestoreDbClusterToPointInTimeRequest.class);
        verify(rdsProxy.client(), times(2)).restoreDBClusterToPointInTime(createCaptor.capture());

        final RestoreDbClusterToPointInTimeRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final RestoreDbClusterToPointInTimeRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }

    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_SetEnableCloudwatchLogsExports() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        final List<String> cloudwatchLogsExports = ImmutableList.of("config-1", "config-2", "config-3");

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME.toBuilder()
                        .enableCloudwatchLogsExports(cloudwatchLogsExports)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<RestoreDbClusterToPointInTimeRequest> restoreCaptor = ArgumentCaptor.forClass(RestoreDbClusterToPointInTimeRequest.class);
        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(restoreCaptor.capture());
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));

        Assertions.assertThat(restoreCaptor.getValue().enableCloudwatchLogsExports()).containsExactlyElementsOf(cloudwatchLogsExports);
    }


    @Test
    public void handleRequest_RestoreDbClusterToPointInTime_ModifyAfterCreate() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());
        when(rdsProxy.client().describeEvents(any(DescribeEventsRequest.class)))
                .thenReturn(DescribeEventsResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEvents(any(DescribeEventsRequest.class));
    }

    @Test
    public void handleRequest_CreateDbCluster_SetDefaultPortForProvisionedPostgresql() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder()
                        .engine(ENGINE_AURORA_POSTGRESQL)
                        .port(null)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbClusterRequest> captor = ArgumentCaptor.forClass(CreateDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).createDBCluster(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(captor.getValue().port()).isEqualTo(3306);
    }

    @Test
    public void handleRequest_CreateDbCluster_SetDefaultPortForServerlessPostgresql() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder()
                        .engine(ENGINE_AURORA_POSTGRESQL)
                        .engineMode(EngineMode.Serverless.toString())
                        .port(null)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateDbClusterRequest> captor = ArgumentCaptor.forClass(CreateDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).createDBCluster(captor.capture());
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(captor.getValue().port()).isEqualTo(5432);
    }

    static class CreateDBClusterExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.StorageTypeNotAvailableFault, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.StorageTypeNotSupportedFault, HandlerErrorCode.InvalidRequest),
                    // Put exception classes below
                    Arguments.of(DbClusterAlreadyExistsException.builder().message(ERROR_MSG).build(), HandlerErrorCode.AlreadyExists),
                    Arguments.of(StorageTypeNotAvailableException.builder().message(ERROR_MSG).build(), HandlerErrorCode.InvalidRequest),
                    Arguments.of(StorageTypeNotSupportedException.builder().message(ERROR_MSG).build(), HandlerErrorCode.InvalidRequest),
                    Arguments.of(DomainNotFoundException.builder().message(ERROR_MSG).build(), HandlerErrorCode.NotFound),
                    Arguments.of(new RuntimeException(ERROR_MSG), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CreateDBClusterExceptionArgumentsProvider.class)
    public void handleRequest_CreateDBCluster_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectCreateDBClusterCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL,
                requestException,
                expectResponseCode
        );
    }

    @Test
    public void handleRequest_CreateDBCluster_DBClusterInTerminalState() {
        final CallbackContext context = new CallbackContext();

        Assertions.assertThatThrownBy(() -> {
            test_handleRequest_base(
                    context,
                    () -> DBCLUSTER_ACTIVE.toBuilder()
                            .status(DBClusterStatus.InaccessibleEncryptionCredentials.toString())
                            .build(),
                    () -> RESOURCE_MODEL,
                    expectFailed(HandlerErrorCode.NotStabilized)
            );
        }).isInstanceOf(CfnNotStabilizedException.class);

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
    }
}
