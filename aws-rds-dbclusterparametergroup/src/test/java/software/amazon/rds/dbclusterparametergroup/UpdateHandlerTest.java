package software.amazon.rds.dbclusterparametergroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    private static final List<Tag> UPDATED_TAG_SET = Lists.newArrayList(Tag.builder().key("updated_key").value("updated_value").build());

    @Mock
    @Getter
    RdsClient rdsClient;

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Getter
    private UpdateHandler handler;

    private ResourceModel RESOURCE_MODEL_PREV;
    private ResourceModel RESOURCE_MODEL;
    private ResourceModel RESOURCE_MODEL_UPD;

    private Map<String, Object> PARAMS;
    private Map<String, Object> UPDATED_PARAMS;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());

        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = new BaseProxyClient<>(proxy, rdsClient);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");

        UPDATED_PARAMS = new HashMap<>();
        UPDATED_PARAMS.put("param", "value");

        RESOURCE_MODEL_PREV = ResourceModel.builder()
                .parameters(null)
                .build();

        RESOURCE_MODEL = ResourceModel.builder()
                .description(DESCRIPTION)
                .dBClusterParameterGroupName("SampleName")
                .family(FAMILY)
                .parameters(PARAMS)
                .tags(TAG_SET)
                .build();

        RESOURCE_MODEL_UPD = ResourceModel.builder()
                .description(UPDATED_DESCRIPTION)
                .dBClusterParameterGroupName("SampleName")
                .family(FAMILY)
                .parameters(UPDATED_PARAMS)
                .tags(UPDATED_TAG_SET)
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
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        when(rdsClient.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder().build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                () -> DBClusterParameterGroup.builder().dbClusterParameterGroupArn(ARN).build(),
                () -> RESOURCE_MODEL_PREV,
                () -> RESOURCE_MODEL_UPD,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_ResetRemovedParameters() {
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        when(rdsClient.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder()
                        .parameters(Parameter.builder().parameterName("parameter1").build()).build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                () -> DBClusterParameterGroup.builder().dbClusterParameterGroupArn(ARN).build(),
                () -> RESOURCE_MODEL_PREV,
                () -> RESOURCE_MODEL_UPD,
                expectSuccess()
        );

        ArgumentCaptor<ResetDbClusterParameterGroupRequest> captor = ArgumentCaptor.forClass(ResetDbClusterParameterGroupRequest.class);
        verify(rdsProxy.client(), times(1)).resetDBClusterParameterGroup(captor.capture());
        Assertions.assertThat(captor.getValue().parameters()).hasSize(1);
        Assertions.assertThat(captor.getValue().parameters().get(0).parameterName()).isEqualTo("parameter1");


        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_NoParametersChanged() {
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                () -> DBClusterParameterGroup.builder().dbClusterParameterGroupArn(ARN).build(),
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL.toBuilder().tags(TAG_SET_ALTER).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));

    }

    @Test
    public void handleRequest_StabilizeDBClusters() {
        when(rdsClient.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class)))
                .thenReturn(ResetDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder()
                        .parameters(translateParamMapToCollection(PARAMS))
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupName("group-name")
                .dbClusterParameterGroupArn(ARN)
                .build();

        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        when(rdsClient.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());

        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final DBCluster dbCluster = DBCluster.builder()
                .dbClusterIdentifier("db-cluster-identifier")
                .dbClusterParameterGroup("group-name")
                .build();

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(dbCluster.toBuilder().status("modifying").build())
                        .build())
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().status("available").build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL.toBuilder().dBClusterParameterGroupName("group-name").parameters(PARAMS).tags(TAG_SET).build(),
                () -> RESOURCE_MODEL_UPD.toBuilder().dBClusterParameterGroupName("group-name").parameters(UPDATED_PARAMS).tags(UPDATED_TAG_SET).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }
}
