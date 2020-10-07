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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

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

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler();
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
    public void handleRequest_SimpleSuccess() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenThrow(DbInstanceNotFoundException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxyClient.client()).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DbInstanceNotFound() {
        final AwsErrorDetails details = AwsErrorDetails.builder()
                .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                .build();
        final DbInstanceNotFoundException exception = DbInstanceNotFoundException.builder().awsErrorDetails(details).build();

        when(rdsProxyClient.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(exception);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(rdsProxyClient.client()).deleteDBInstance(any(DeleteDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_OtherException() {
        when(rdsProxyClient.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(new RuntimeException());
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BLDR().build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(rdsProxyClient.client()).deleteDBInstance(any(DeleteDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_IsDeleting() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxyClient.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final DescribeDbInstancesResponse describeDbInstancesResponseDeleting = DescribeDbInstancesResponse.builder()
                .dbInstances(DB_INSTANCE_DELETING)
                .build();

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
            if (fetchedOnce.compareAndSet(false, true)) {
                return describeDbInstancesResponseDeleting;
            }
            throw DbInstanceNotFoundException.builder().build();
        });

        final ResourceModel resourceModel = RESOURCE_MODEL_BLDR().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .logicalResourceIdentifier(resourceModel.getDBInstanceIdentifier())
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsProxyClient.client()).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxyClient.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SkipFinalSnapshot() {
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenThrow(DbInstanceNotFoundException.builder().build());

        final ResourceModel resourceModel = RESOURCE_MODEL_BLDR().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .logicalResourceIdentifier(resourceModel.getDBInstanceIdentifier())
                .snapshotRequested(false) // leaving it out as null is not enough: it's a boxed type
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setDeleting(true);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
