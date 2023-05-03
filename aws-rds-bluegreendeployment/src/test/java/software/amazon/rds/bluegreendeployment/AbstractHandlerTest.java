package software.amazon.rds.bluegreendeployment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsRequest;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.MethodCallExpectation;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<BlueGreenDeployment, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "blue-green-deployment";

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJSONObject();

    protected static final LoggerProxy logger = new LoggerProxy();

    protected static final RequestLogger EMPTY_REQUEST_LOGGER = new RequestLogger(logger, ResourceHandlerRequest.builder().build(), null);

    protected static final Credentials MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), EMPTY_REQUEST_LOGGER);
    }

    @Override
    protected void expectResourceSupply(final Supplier<BlueGreenDeployment> supplier) {
        expectDescribeBlueGreenDeploymentsCall().setup().then(res -> DescribeBlueGreenDeploymentsResponse.builder()
                .blueGreenDeployments(supplier.get())
                .build());
    }

    protected MethodCallExpectation<DescribeBlueGreenDeploymentsRequest, DescribeBlueGreenDeploymentsResponse> expectDescribeBlueGreenDeploymentsCall() {
        return new MethodCallExpectation<DescribeBlueGreenDeploymentsRequest, DescribeBlueGreenDeploymentsResponse>() {
            @Override
            public OngoingStubbing<DescribeBlueGreenDeploymentsResponse> setup() {
                return when(getRdsProxy().client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)));
            }

            @Override
            public ArgumentCaptor<DescribeBlueGreenDeploymentsRequest> verify() {
                ArgumentCaptor<DescribeBlueGreenDeploymentsRequest> captor = ArgumentCaptor.forClass(DescribeBlueGreenDeploymentsRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).describeBlueGreenDeployments(captor.capture());
                return captor;
            }
        };
    }

    static ProxyClient<RdsClient> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final RdsClient rdsClient) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    public abstract HandlerName getHandlerName();
}
