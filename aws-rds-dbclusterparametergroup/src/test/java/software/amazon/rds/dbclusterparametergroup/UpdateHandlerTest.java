package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.paginators.DescribeDBClusterParametersIterable;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {
    private static final List<Tag> UPDATED_TAG_SET = Lists.newArrayList(Tag.builder().key("updated_key").value("updated_value").build());
    ;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private UpdateHandler handler;

    private ResourceModel RESOURCE_MODEL_PREV;
    private ResourceModel RESOURCE_MODEL;
    private ResourceModel UPDATED_RESOURCE_MODEL;

    private Map<String, Object> PARAMS;
    private Map<String, Object> UPDATED_PARAMS;

    private ResourceHandlerRequest<ResourceModel> requestSameParams;
    private ResourceHandlerRequest<ResourceModel> requestUpdParams;

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @BeforeEach
    public void setup() {

        handler = new UpdateHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);


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
        UPDATED_RESOURCE_MODEL = ResourceModel.builder()
                .description(UPDATED_DESCRIPTION)
                .dBClusterParameterGroupName("SampleName")
                .family(FAMILY)
                .parameters(UPDATED_PARAMS)
                .tags(UPDATED_TAG_SET)
                .build();

        requestSameParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
        requestUpdParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(UPDATED_RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL_PREV)
                .desiredResourceTags(translateTagsToMap(UPDATED_TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
    }

    @Test
    public void handleRequest_SimpleSuccess() {

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn").build()).build();
        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();


        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));


        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }


    private void mockDescribeDbClusterParametersResponse(String firstParamApplyType,
                                                         String secondParamApplyType,
                                                         boolean isModifiable) {
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

        final DescribeEngineDefaultClusterParametersResponse describeEngineDefaultParametersResponse = DescribeEngineDefaultClusterParametersResponse.builder()
                .engineDefaults(EngineDefaults.builder()
                        .parameters(param1, param2)
                        .build()
                ).build();
        when(proxyRdsClient.client().describeEngineDefaultClusterParameters(any(DescribeEngineDefaultClusterParametersRequest.class)))
                .thenReturn(describeEngineDefaultParametersResponse);

        if (!isModifiable)
            return;

        final DescribeDbClusterParametersResponse describeDbClusterParametersResponse = DescribeDbClusterParametersResponse.builder().marker(null)
                .parameters(param1, param2).build();

        final DescribeDBClusterParametersIterable describeDbClusterParametersIterable = mock(DescribeDBClusterParametersIterable.class);
        when(describeDbClusterParametersIterable.stream())
                .thenReturn(Stream.<DescribeDbClusterParametersResponse>builder()
                        .add(describeDbClusterParametersResponse)
                        .build()
                );
        when(proxyRdsClient.client().describeDBClusterParametersPaginator(any(DescribeDbClusterParametersRequest.class))).thenReturn(describeDbClusterParametersIterable);
    }

}
