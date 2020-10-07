package software.amazon.rds.dbinstance;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

public class BaseProxyClient<ClientT> implements ProxyClient<ClientT> {

    protected final AmazonWebServicesClientProxy proxy;
    protected final ClientT client;

    public BaseProxyClient(
            final AmazonWebServicesClientProxy proxy,
            final ClientT client
    ) {
        this.proxy = proxy;
        this.client = client;
    }

    @Override
    public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
        return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
    }

    @Override
    public <RequestT extends AwsRequest, ResponseT extends AwsResponse> CompletableFuture<ResponseT> injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>> IterableT injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
        return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
    }

    @Override
    public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT> injectCredentialsAndInvokeV2InputStream(RequestT request, Function<RequestT, ResponseInputStream<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT> injectCredentialsAndInvokeV2Bytes(RequestT request, Function<RequestT, ResponseBytes<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientT client() {
        return client;
    }
}
