package software.amazon.rds.dbsnapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.test.AbstractTestBase;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBSnapshot, ResourceModel, CallbackContext> {
    protected static final LoggerProxy logger;
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final String MSG_NOT_FOUND = "DBSnapshot not found";
    private static final String DB_SNAPSHOT_IDENTIFIER = "db-snapshot-identifier";

    private static final String DB_INSTANCE_IDENTIFIER = "db-instance-identifier";

    protected static final List<Tag> TAG_LIST_EMPTY;
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

        TAG_LIST_EMPTY = Collections.emptyList();
        TAG_LIST = Arrays.asList(
                Tag.builder().key("foo-1").value("bar-1").build(),
                Tag.builder().key("foo-2").value("bar-2").build(),
                Tag.builder().key("foo-3").value("bar-3").build()
        );

        TAG_LIST_ALTER = Arrays.asList(
                Tag.builder().key("foo-4").value("bar-4").build(),
                Tag.builder().key("foo-5").value("bar-5").build()
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
    protected static final DBSnapshot DB_SNAPSHOT_AVAILABLE;
    protected static final DBSnapshot DB_SNAPSHOT_CREATING;
    protected static final DBSnapshot DB_SNAPSHOT_MODIFYING;
    protected static final DBSnapshot DB_SNAPSHOT_DELETING;
    protected static final String LOGICAL_IDENTIFIER = "DBSnapshotLogicalId";


    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    static {
        RESOURCE_MODEL = RESOURCE_MODEL_BUILDER()
                .build();

        DB_SNAPSHOT_AVAILABLE = DBSnapshot.builder()
                .dbSnapshotIdentifier("dbSnapshotIdentifier")
                .dbInstanceIdentifier("dbInstanceIdentifier")
                .status("available")
                .dbSnapshotArn("db-snapshot-arn")
                .build();

        DB_SNAPSHOT_CREATING = DBSnapshot.builder()
                .dbSnapshotIdentifier("dbSnapshotIdentifier")
                .dbInstanceIdentifier("dbInstanceIdentifier")
                .status("creating")
                .dbSnapshotArn("db-snapshot-arn")
                .build();

        DB_SNAPSHOT_MODIFYING = DBSnapshot.builder()
                .dbSnapshotIdentifier("dbSnapshotIdentifier")
                .dbInstanceIdentifier("dbInstanceIdentifier")
                .status("modifying")
                .dbSnapshotArn("db-snapshot-arn")
                .build();

        DB_SNAPSHOT_DELETING = DBSnapshot.builder()
                .dbSnapshotIdentifier("dbSnapshotIdentifier")
                .dbInstanceIdentifier("dbInstanceIdentifier")
                .status("deleting")
                .dbSnapshotArn("db-snapshot-arn")
                .build();
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BUILDER() {
        return ResourceModel.builder()
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER);
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
    protected void expectResourceSupply(Supplier<DBSnapshot> supplier) {

        when(getRdsProxy().client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class))).then(res -> {
            DBSnapshot resource = supplier.get();

            Collection<DBSnapshot> snapshots;
            if (resource != null) {
                snapshots = Collections.singletonList(resource);
            } else {
                snapshots = Collections.emptyList();
            }
            return DescribeDbSnapshotsResponse.builder().dbSnapshots(snapshots).build();
        });
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), logger);
    }

    protected Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }
}
