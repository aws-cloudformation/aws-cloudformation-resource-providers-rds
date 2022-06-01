package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.test.AbstractTestBase;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBClusterEndpoint, ResourceModel, CallbackContext> {
    protected static final LoggerProxy logger;
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final String MSG_NOT_FOUND_ERR = "Cluster Endpoint not found";



    static {
        logger = new LoggerProxy();
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
    }
    protected static final ResourceModel RESOURCE_MODEL;
    protected static final DBClusterEndpoint DB_CLUSTER_ENDPOINT_AVAILABLE;
    protected static final String LOGICAL_IDENTIFIER = "DBClusterEndpointLogicalId";

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getProxyClient();

    static {
        RESOURCE_MODEL = ResourceModel.builder()
                .dBClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dBClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .build();

        DB_CLUSTER_ENDPOINT_AVAILABLE = DBClusterEndpoint.builder()
                .dbClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dbClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .status("available")
                .build();
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_IDENTIFIER;
    }

    static ProxyClient<RdsClient> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final RdsClient rdsClient
    ) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request,
                                              Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT request, Function<RequestT, ResponseInputStream<ResponseT>> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2InputStream(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT request, Function<RequestT, ResponseBytes<ResponseT>> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2Bytes(request, requestFunction);
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    @Override
    protected void expectResourceSupply(Supplier<DBClusterEndpoint> supplier) {
        when(getProxyClient()
                .client()
                .describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class))
        ).then(res -> DescribeDbClusterEndpointsResponse.builder()
                .dbClusterEndpoints(supplier.get())
                .build()
        );
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getProxyClient(), logger);
    }
}
