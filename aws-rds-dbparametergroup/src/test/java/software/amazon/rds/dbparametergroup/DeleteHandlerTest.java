package software.amazon.rds.dbparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbParameterGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rdsClient;

    private DeleteHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyRdsClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DeleteDbParameterGroupResponse deleteDBParameterGroupResponse = DeleteDbParameterGroupResponse.builder().build();
        when(rdsClient.deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class))).thenReturn(deleteDBParameterGroupResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessNotFound() {
        DbParameterGroupNotFoundException dbParameterGroupNotFoundException = DbParameterGroupNotFoundException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().build()).build();

        when(rdsClient.deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class)))
                .thenThrow(dbParameterGroupNotFoundException);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder()
                        .build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(proxyRdsClient.client()).deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleInvalid() {
        final DeleteDbParameterGroupResponse deleteDBParameterGroupResponse = DeleteDbParameterGroupResponse.builder().build();
        when(rdsClient.deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class)))
                .thenThrow(InvalidParameterException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder()
                        .build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(proxyRdsClient.client()).deleteDBParameterGroup(any(DeleteDbParameterGroupRequest.class));
    }
}
