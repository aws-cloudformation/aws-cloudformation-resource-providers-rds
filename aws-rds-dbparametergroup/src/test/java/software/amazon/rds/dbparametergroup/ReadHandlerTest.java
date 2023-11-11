package software.amazon.rds.dbparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.verification.AccessPermissionAlias;
import software.amazon.rds.test.common.verification.AccessPermissionFactory;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = new BaseProxyClient<>(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(
                rdsClient,
                new AccessPermissionAlias(
                        AccessPermissionFactory.fromString("rds:DescribeEngineDefaultParametersPaginator"),
                        AccessPermissionFactory.fromString("rds:DescribeEngineDefaultParameters")
                ),
                new AccessPermissionAlias(
                        AccessPermissionFactory.fromString("rds:DescribeDBParametersPaginator"),
                        AccessPermissionFactory.fromString("rds:DescribeDBParameters")
                )
        );
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(proxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class)))
                .thenReturn(DescribeDbParameterGroupsResponse.builder().dbParameterGroups(DB_PARAMETER_GROUP_ACTIVE).build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().tagList(Tag.builder().key("Key").value("Value").build()).build());

        expectEmptyDescribeParametersResponse(proxyClient);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().dBParameterGroupName("testDBParameterGroupName").build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext(), EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client()).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
        verify(proxyClient.client()).describeDBParameters(any(DescribeDbParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleNotFound() {
        when(proxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class)))
                .thenThrow(DbParameterGroupNotFoundException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().build())
                        .build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().dBParameterGroupName("testDBParameterGroupName").build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext(), EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
    }

    @Test
    public void handleRequest_SimpleException() {
        when(proxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class)))
                .thenThrow(InvalidParameterException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().dBParameterGroupName("testDBParameterGroupName").build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext(), EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
    }

    @Test
    public void handleRequest_ReadModifiedParameters() {
        when(proxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class)))
                .thenReturn(DescribeDbParameterGroupsResponse.builder().dbParameterGroups(DB_PARAMETER_GROUP_ACTIVE).build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().tagList(Tag.builder().key("Key").value("Value").build()).build());

        when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class)))
                .thenReturn(DescribeDbParametersResponse.builder()
                        .parameters(
                                Parameter.builder().parameterName("param1").parameterValue("value1").build(),
                                Parameter.builder().parameterName("param2").parameterValue("value2-modified").build(),
                                Parameter.builder().parameterName("param3").build()
                        )
                        .marker(null)
                        .build());

        when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class)))
                .thenReturn(DescribeEngineDefaultParametersResponse.builder()
                        .engineDefaults(EngineDefaults.builder()
                                .parameters(
                                        Parameter.builder().parameterName("param1").parameterValue("value1").build(),
                                        Parameter.builder().parameterName("param2").parameterValue("value2").build(),
                                        Parameter.builder().parameterName("param3").build()
                                )
                                .marker(null)
                                .build())
                        .build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().dBParameterGroupName("testDBParameterGroupName").build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext(), EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getParameters())
                .isEqualTo(ImmutableMap.of(
                        "param2", "value2-modified"
                ));

        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client()).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
        verify(proxyClient.client()).describeDBParameters(any(DescribeDbParametersRequest.class));
    }
}
