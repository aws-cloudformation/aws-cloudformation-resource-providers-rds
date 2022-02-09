package software.amazon.rds.common.logging;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.cloudformation.proxy.ProxyClient;

public class ProxyClientLogger {

    public static <ClientT> ProxyClient<ClientT> newProxy(final RequestLogger requestLogger,
                                                          final ProxyClient<ClientT> proxyClient) {
        return new ProxyClient<ClientT>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                ResponseT result = getResponseT(requestLogger, request, requestFunction,
                        (req, reqFunction) -> proxyClient.injectCredentialsAndInvokeV2(req, reqFunction));
                requestLogger.log(result.getClass().getSimpleName(), result);
                return result;
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request,
                                              Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                CompletableFuture<ResponseT> result = getResponseT(requestLogger, request, requestFunction,
                        (req, reqFunction) -> proxyClient.injectCredentialsAndInvokeV2Async(req, reqFunction));
                requestLogger.log(result.getClass().getSimpleName(), result);
                return result;
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                IterableT result = getResponseT(requestLogger, request, requestFunction,
                        (req, reqFunction) -> proxyClient.injectCredentialsAndInvokeIterableV2(req, reqFunction));
                result.stream().forEach(response -> requestLogger.log(response.getClass().getSimpleName(), response));
                return result;
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT request,
                                                    Function<RequestT, ResponseInputStream<ResponseT>> requestFunction) {
                ResponseInputStream<ResponseT> result = getResponseT(requestLogger, request, requestFunction,
                        (req, reqFunction) -> proxyClient.injectCredentialsAndInvokeV2InputStream(req, reqFunction));
                requestLogger.log(result.response().getClass().getSimpleName(), result.response());
                return result;
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT request,
                                              Function<RequestT, ResponseBytes<ResponseT>> requestFunction) {
                ResponseBytes<ResponseT> result = getResponseT(requestLogger, request, requestFunction,
                        (req, reqFunction) -> proxyClient.injectCredentialsAndInvokeV2Bytes(req, reqFunction));
                requestLogger.log(result.response().getClass().getSimpleName(), result.response());
                return result;
            }

            @Override
            public ClientT client() {
                return proxyClient.client();
            }
        };
    }

    private static <RequestT extends AwsRequest, ResultT> ResultT getResponseT(
            final RequestLogger requestLogger, final RequestT request,
            final Function<RequestT, ResultT> requestFunction,
            final BiFunction<RequestT, Function<RequestT, ResultT>, ResultT> injectCredentials) {
        ResultT result = null;
        try {
            requestLogger.log(request.getClass().getSimpleName(), request);
            result = injectCredentials.apply(request, requestFunction);
        } catch (Exception e) {
            requestLogger.logAndThrow(e);
        }
        return result;
    }
}
