package software.amazon.rds.dbclusterendpoint;

import com.google.common.collect.ImmutableSet;
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
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.test.AbstractTestBase;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBClusterEndpoint, ResourceModel, CallbackContext> {
    protected static final LoggerProxy logger;
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final String MSG_NOT_FOUND = "Cluster Endpoint not found";
    private static final String DB_CLUSTER_ENDPOINT_IDENTIFIER = "db-cluster-endpoint-identifier";
    private static final String DB_CLUSTER_ENDPOINT_TYPE = "ANY";
    private static final String DB_CLUSTER_IDENTIFIER = "db-cluster-identifier";

    protected static final Set<Tag> TAG_LIST_EMPTY;
    protected static final Set<Tag> TAG_LIST;
    protected static final Set<Tag> TAG_LIST_ALTER;
    protected static final Tagging.TagSet TAG_SET;

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofMillis(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();


    static {
        logger = new LoggerProxy();
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        TAG_LIST_EMPTY = ImmutableSet.of();
        TAG_LIST = ImmutableSet.of(
                Tag.builder().key("foo-1").value("bar-1").build(),
                Tag.builder().key("foo-2").value("bar-2").build(),
                Tag.builder().key("foo-3").value("bar-3").build()
        );

        TAG_LIST_ALTER = ImmutableSet.of(
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
    protected static final ResourceModel RESOURCE_MODEL_WITH_TAGS;

    protected static final DBClusterEndpoint DB_CLUSTER_ENDPOINT_AVAILABLE;
    protected static final DBClusterEndpoint DB_CLUSTER_ENDPOINT_CREATING;
    protected static final DBClusterEndpoint DB_CLUSTER_ENDPOINT_DELETING;
    protected static final String LOGICAL_IDENTIFIER = "DBClusterEndpointLogicalId";

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    static {
        RESOURCE_MODEL = ResourceModel.builder()
                .dBClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dBClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .build();

        RESOURCE_MODEL_WITH_TAGS = ResourceModel.builder()
                .dBClusterIdentifier("dbClusterEndpointIdentifier")
                .dBClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .tags(TAG_LIST_ALTER)
                .build();

        DB_CLUSTER_ENDPOINT_AVAILABLE = DBClusterEndpoint.builder()
                .dbClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dbClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .status("available")
                .dbClusterEndpointArn("db-cluster-endpoint-arn")
                .build();

        DB_CLUSTER_ENDPOINT_CREATING = DBClusterEndpoint.builder()
                .dbClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dbClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .status("creating")
                .dbClusterEndpointArn("db-cluster-endpoint-arn")
                .build();

        DB_CLUSTER_ENDPOINT_DELETING = DBClusterEndpoint.builder()
                .dbClusterEndpointIdentifier("dbClusterEndpointIdentifier")
                .dbClusterIdentifier("clusterIdentifier")
                .endpointType("ANY")
                .status("deleting")
                .dbClusterEndpointArn("db-cluster-endpoint-arn")
                .build();
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BUILDER_WITH_TAGS() {
        return RESOURCE_MODEL_BUILDER()
                .tags(TAG_LIST);
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BUILDER() {
        return ResourceModel.builder()
                .dBClusterEndpointIdentifier(DB_CLUSTER_ENDPOINT_IDENTIFIER)
                .endpointType(DB_CLUSTER_ENDPOINT_TYPE)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER);
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

        when(getRdsProxy().client().describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class))).then(res -> {
            DBClusterEndpoint resource = supplier.get();

            Collection<DBClusterEndpoint> endpoints;
            if (resource != null) {
                endpoints = Collections.singletonList(resource);
            } else {
                endpoints = Collections.emptyList();
            }
            return DescribeDbClusterEndpointsResponse.builder().dbClusterEndpoints(endpoints).build();
        });
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), logger);
    }
}
