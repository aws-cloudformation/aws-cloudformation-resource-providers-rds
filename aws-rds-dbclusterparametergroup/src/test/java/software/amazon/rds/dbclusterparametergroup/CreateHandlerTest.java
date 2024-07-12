package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
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
    @Getter
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    private ResourceModel RESOURCE_MODEL;

    private Map<String, Object> PARAMS;

    /**
     * When building the DbClusterParameters (e.g. for mocking a DescribeDbClusterParametersResponse), is important the order that
     * they get iterated. For example, we have cases where the DbClusterParameters have to be split following some rules, and
     * the insertion order becomes important.
     *
     * This class will allow you to create DbClusterParameters preserving the insertion order when iterating over it.
     */
    private static class InsertionOrderedParamBuilder {
        private final ImmutableMap.Builder<String, Object> parameters = new ImmutableMap.Builder<>();
        private final AtomicInteger parameterIndex = new AtomicInteger(0);

        public static InsertionOrderedParamBuilder builder() {
            return new InsertionOrderedParamBuilder();
        }

        public InsertionOrderedParamBuilder addParameter(String parameterName, String parameterValue) {
            parameters.put(parameterName, parameterValue);
            parameterIndex.incrementAndGet();
            return this;
        }

        public InsertionOrderedParamBuilder addRandomParameters(int numberOfRandomParameters) {
            IntStream.range(0, numberOfRandomParameters).forEach(i -> parameters.put("param" + parameterIndex.incrementAndGet(), "value"));
            return this;
        }

        public Map<String, Object> build() {
            return parameters.build();
        }
    }

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());

        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");


        RESOURCE_MODEL = ResourceModel.builder()
                .description(DESCRIPTION)
                .dBClusterParameterGroupName(null)
                .family(FAMILY)
                .parameters(PARAMS)
                .tags(TAG_SET)
                .build();
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_Success() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_UnauthorizedTaggingOperationOnCreate() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenThrow(RdsException.builder()
                        .message("Role not authorized to execute rds:AddTagsToResource")
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString())
                                .build()
                        ).build());


        Map<String, String> stackTags = ImmutableMap.of(TAG_KEY, TAG_VALUE);
        Map<String, String> systemTags = ImmutableMap.of("systemTag1", "value2", "systemTag2", "value3");
        Map<String, String> allTags = ImmutableMap.<String, String>builder()
                .putAll(stackTags)
                .putAll(systemTags)
                .build();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                callbackContext,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(systemTags)
                        .desiredResourceTags(stackTags),
                null,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(null)
                        .build(),
                expectFailed(HandlerErrorCode.UnauthorizedTaggingOperation)
        );
        ArgumentCaptor<CreateDbClusterParameterGroupRequest> createCaptor = ArgumentCaptor.forClass(CreateDbClusterParameterGroupRequest.class);
        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(createCaptor.capture());

        final CreateDbClusterParameterGroupRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(asSdkTagArray(allTags));
    }

    @Test
    public void handleRequest_Success_timestamp() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        Instant start = Instant.ofEpochSecond(0);
        callbackContext.timestampOnce("START", start);
        Duration timeToAdd = Duration.ofSeconds(50);
        Instant end = Instant.ofEpochSecond(0).plus(timeToAdd);
        callbackContext.timestamp("END", start);
        callbackContext.timestamp("END", end);

        test_handleRequest_base(
                callbackContext,
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        assertThat(callbackContext.getTimestamp("START").isBefore(Instant.now())).isTrue();
        assertThat(callbackContext.getTimestamp("START")).isEqualTo(start);
        assertThat(callbackContext.getTimestamp("END")).isEqualTo(end);
    }
    @Test
    public void handleRequest_Success_timeDelta() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.timestampOnce("START", Instant.now());
        callbackContext.timestamp("END", Instant.now());


        final Instant startTime = Instant.ofEpochSecond(0);
        final Instant currentTime = Instant.ofEpochSecond(60);
        callbackContext.calculateTimeDeltaInMinutes("TimeDeltaTest", currentTime, startTime);

        test_handleRequest_base(
                callbackContext,
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        assertThat(callbackContext.getTimeDelta().get("TimeDeltaTest")).isEqualTo(1.00);
    }

    @Test
    public void handleRequest_FailWithAccessDenied() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorMessage("Access denied")
                                .errorCode(HandlerErrorCode.AccessDenied.toString())
                                .build())
                        .build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_MissingDescribeDBClusterParameterGroupsPermission() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class)))
                .thenThrow(RdsException.builder().awsErrorDetails(
                        AwsErrorDetails.builder().errorCode(HandlerErrorCode.AccessDenied.toString()).build()
                ).build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_ThrottleOnDescribeDBClusters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(HandlerErrorCode.Throttling.toString())
                                .build())
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.Throttling)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_SuccessWithParameters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build()
                );

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    /**
     * There are combinations of ParameterGroups that ModifyDBParameterGroup expected them come in the same request,
     * as they are related and configure a particular functionality. At the same time, the ModifyDBParameterGroup API
     * expects a maximum of 20 parameters that can be modified in a single request.
     * (https://docs.aws.amazon.com/cli/latest/reference/rds/modify-db-parameter-group.html)
     *
     * In the case that the parameters included in the CFN template exceed the request limit, we could end up in
     * a condition in which 2 related parameters existing in the CFN template, end up in 2 different requests.
     * We need to ensure that all related parameters are sent in the same request as defined
     * in BaseHandlerStd.PARAMETER_DEPENDENCIES
     *
     * This test ensure that "aurora_enhanced_binlog", "binlog_backup" and "binlog_replication_globaldb" get bundled in
     * the same request after the split logic that happens in BaseHandlerStd.modifyParameters
     */
    @Test
    public void handleRequest_SuccessSplitParameters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build()
                );

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        // Defining 2 parameters that have to be bundled in the same request. We do this by including them in an
        // insert-ordered Map, and being far away one from each other. With these conditions, they would naturally
        // be bundled in 2 different API requests to "modify dbclusterparametergroup".
        ResourceModel resourceModel = RESOURCE_MODEL.toBuilder()
                .parameters(InsertionOrderedParamBuilder.builder()
                        .addParameter("aurora_enhanced_binlog", "1")
                        .addRandomParameters(BaseHandlerStd.MAX_PARAMETERS_PER_REQUEST)
                        .addParameter("binlog_backup", "0")
                        .addRandomParameters(BaseHandlerStd.MAX_PARAMETERS_PER_REQUEST)
                        .addParameter("binlog_replication_globaldb", "0")
                        .build())
                .build();
        mockDescribeDbClusterParametersResponse(resourceModel.getParameters(), true, "static");

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> resourceModel,
                expectSuccess()
        );

        ArgumentCaptor<ModifyDbClusterParameterGroupRequest> captor = ArgumentCaptor.forClass(ModifyDbClusterParameterGroupRequest.class);

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(3)).modifyDBClusterParameterGroup(captor.capture());
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));

        ModifyDbClusterParameterGroupRequest firstRequest = captor.getAllValues().get(0);
        assertThat(verifyParameterExistsInRequest("aurora_enhanced_binlog", firstRequest)).isEqualTo(true);
        assertThat(verifyParameterExistsInRequest("binlog_backup", firstRequest)).isEqualTo(true);
        assertThat(verifyParameterExistsInRequest("binlog_replication_globaldb", firstRequest)).isEqualTo(true);
    }

    @Test
    public void handleRequest_SuccessWithEmptyParameters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL.toBuilder()
                        .parameters(Collections.emptyMap())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_InProgressFailedUnmodifiableParams() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", false);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InvalidRequest)
        );

        assertThat(response.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: param");

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    @Test
    public void handleRequest_InProgressFailedUnsupportedParams() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().parameters(Collections.singletonMap("Wrong Key", "Wrong value")).build(),
                expectFailed(HandlerErrorCode.InvalidRequest)
        );

        assertThat(response.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: Wrong Key");

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    @Test
    public void handleRequest_ThrottlingFailure() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenThrow(RdsException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(HandlerErrorCode.Throttling.toString())
                                .build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.Throttling)
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    private void mockDescribeDbClusterParametersResponse(
            Map<String, Object> params,
            final boolean isModifiable,
            final String paramApplyType
    ) {
        List<DescribeDbClusterParametersResponse> responses = new ArrayList<>();
        Lists.partition(params.keySet().stream().toList(), BaseHandlerStd.MAX_PARAMETERS_PER_REQUEST).forEach(page -> {
            List<Parameter> parameters = new ArrayList<>();
            page.forEach(paramName -> {
                parameters.add(Parameter.builder()
                        .parameterName(paramName)
                        .parameterValue("system_value")
                        .isModifiable(isModifiable)
                        .applyType(paramApplyType)
                        .build());
            });
            responses.add(DescribeDbClusterParametersResponse.builder()
                    .marker("marker")
                    .parameters(parameters).build());
        });
        responses.add(DescribeDbClusterParametersResponse.builder().build());

        doAnswer(AdditionalAnswers.returnsElementsOf(responses))
                .when(rdsProxy.client()).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    private void mockDescribeDbClusterParametersResponse(
            final String firstParamApplyType,
            final String secondParamApplyType,
            final boolean isModifiable
    ) {
        Parameter param1 = Parameter.builder()
                .parameterName("param")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(firstParamApplyType)
                .build();
        Parameter param2 = Parameter.builder()
                .parameterName("param2")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();

        final DescribeDbClusterParametersResponse firstPage = DescribeDbClusterParametersResponse.builder()
                .marker("marker")
                .parameters(param1, param2).build();


        final DescribeDbClusterParametersResponse lastPage = DescribeDbClusterParametersResponse.builder().build();

        when(rdsProxy.client().describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(firstPage)
                .thenReturn(lastPage);
    }

    private Tag[] asSdkTagArray(final Map<String, String> tags) {
        return Iterables.toArray(Tagging.translateTagsToSdk(tags), software.amazon.awssdk.services.rds.model.Tag.class);
    }

    private boolean verifyParameterExistsInRequest(String parameterName, ModifyDbClusterParameterGroupRequest modifyDbClusterParameterGroupRequest) {
        return modifyDbClusterParameterGroupRequest.parameters().stream()
                .anyMatch(parameter -> parameter.parameterName().equals(parameterName));
    }
}
