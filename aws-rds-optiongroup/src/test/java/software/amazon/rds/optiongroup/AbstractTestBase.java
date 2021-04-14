package software.amazon.rds.optiongroup;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final LoggerProxy logger;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final OptionGroup OPTION_GROUP_ACTIVE;
    protected static final Set<Tag> TAG_SET;

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        logger = new LoggerProxy();

        RESOURCE_MODEL = ResourceModel.builder()
                .optionGroupName("testOptionGroup")
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("testOptionConfiguration")
                                .optionVersion("1.2.3")
                                .build()
                ))
                .build();

        OPTION_GROUP_ACTIVE = OptionGroup.builder()
                .optionGroupArn("arn")
                .optionGroupName("testOptionGroup")
                .build();
        TAG_SET = Sets.newSet(Tag.builder().key("key").value("value").build());
    }

    static Map<String, String> translateTagsToMap(final Set<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
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
}
