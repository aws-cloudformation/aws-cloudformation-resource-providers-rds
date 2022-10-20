package software.amazon.rds.customdbengineversion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.MethodCallExpectation;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBEngineVersion, ResourceModel, CallbackContext> {
    protected static final LoggerProxy logger;
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final String MSG_NOT_FOUND = "DB Engine Version not found";
    private static final String ENGINE_VERSION = "19.oracle-custom-engine";
    private static final String ENGINE = "oracle";
    private static final String ARN = "arn";

    protected static final Set<Tag> TAG_LIST_EMPTY;
    protected static final List<Tag> TAG_LIST;
    protected static final List<Tag> TAG_LIST_ALTER;
    protected static final Tagging.TagSet TAG_SET;

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofMillis(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();


    static {
        logger = new LoggerProxy();
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        TAG_LIST_EMPTY = ImmutableSet.of();
        TAG_LIST = Arrays.asList(
                Tag.builder().key("key-1").value("value-1").build(),
                Tag.builder().key("key-2").value("value-2").build(),
                Tag.builder().key("key-3").value("value-3").build()
        );

        TAG_LIST_ALTER = Arrays.asList(
                Tag.builder().key("key-4").value("value-4").build(),
                Tag.builder().key("key-5").value("value-5").build()
        );

        TAG_SET = Tagging.TagSet.builder()
                .systemTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-1").value("system-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-2").value("system-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-3").value("system-tag-value3").build()
                )).stackTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-3").value("stack-tag-value3").build()
                )).resourceTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-1").value("resource-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-2").value("resource-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-3").value("resource-tag-value3").build()
                )).build();
    }

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final DBEngineVersion DB_ENGINE_VERSION_AVAILABLE;
    protected static final DBEngineVersion DB_ENGINE_VERSION_CREATING;
    protected static final DBEngineVersion DB_ENGINE_VERSION_MODIFYING;
    protected static final DBEngineVersion DB_ENGINE_VERSION_DELETING;
    protected static final String LOGICAL_IDENTIFIER = "DBEngineVersionLogicalId";

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJSONObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    static {
        RESOURCE_MODEL = RESOURCE_MODEL_BUILDER()
                .build();

        DB_ENGINE_VERSION_AVAILABLE = DBEngineVersion.builder()
                .engineVersion("engine-version")
                .engine("engine")
                .dbEngineVersionArn("engine-version-arn")
                .status("available")
                .build();

        DB_ENGINE_VERSION_CREATING = DBEngineVersion.builder()
                .engineVersion("engine-version")
                .engine("engine")
                .dbEngineVersionArn("engine-version-arn")
                .status("creating")
                .build();

        DB_ENGINE_VERSION_MODIFYING = DBEngineVersion.builder()
                .engineVersion("engine-version")
                .engine("engine")
                .dbEngineVersionArn("engine-version-arn")
                .status("modifying")
                .build();

        DB_ENGINE_VERSION_DELETING = DBEngineVersion.builder()
                .engineVersion("engine-version")
                .engine("engine")
                .dbEngineVersionArn("engine-version-arn")
                .status("deleting")
                .build();
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BUILDER() {
        return ResourceModel.builder()
                .engineVersion(ENGINE_VERSION)
                .engine(ENGINE)
                .dBEngineVersionArn(ARN)
                .status("available");
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_IDENTIFIER;
    }

    static ProxyClient<RdsClient> mockProxy(
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
            injectCredentialsAndInvokeV2InputStream(RequestT request,
                                                    Function<RequestT, ResponseInputStream<ResponseT>> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2InputStream(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT request,
                                              Function<RequestT, ResponseBytes<ResponseT>> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2Bytes(request, requestFunction);
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    @Override
    protected void expectResourceSupply(final Supplier<DBEngineVersion> supplier) {
        expectDescribeDBClustersCall().setup().then(res -> DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(supplier.get())
                .build());
    }

    protected MethodCallExpectation<DescribeDbEngineVersionsRequest, DescribeDbEngineVersionsResponse> expectDescribeDBClustersCall() {
        return new MethodCallExpectation<DescribeDbEngineVersionsRequest, DescribeDbEngineVersionsResponse>() {
            @Override
            public OngoingStubbing<DescribeDbEngineVersionsResponse> setup() {
                return when(getRdsProxy().client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)));
            }

            @Override
            public ArgumentCaptor<DescribeDbEngineVersionsRequest> verify() {
                ArgumentCaptor<DescribeDbEngineVersionsRequest> captor = ArgumentCaptor.forClass(DescribeDbEngineVersionsRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).describeDBEngineVersions(captor.capture());
                return captor;
            }
        };
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), logger);
    }
}
