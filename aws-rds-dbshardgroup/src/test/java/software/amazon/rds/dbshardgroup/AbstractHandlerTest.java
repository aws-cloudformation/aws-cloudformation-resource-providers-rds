package software.amazon.rds.dbshardgroup;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsResponse;
import software.amazon.cloudformation.proxy.*;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.AbstractTestBase;

public abstract class AbstractHandlerTest extends AbstractTestBase <DBShardGroup, ResourceModel, CallbackContext> {

  protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbshardgroup";
  protected static final String CLIENT_REQUEST_TOKEN = UUID.randomUUID().toString();
  protected static final String STACK_ID = UUID.randomUUID().toString();

  protected static final Credentials MOCK_CREDENTIALS;
  protected static final LoggerProxy logger;
  static final Set<Tag> TAG_LIST;
  static final Set<Tag> TAG_LIST_EMPTY;
  static final Set<Tag> TAG_LIST_ALTER;
  static final Tagging.TagSet TAG_SET;

  static {
    MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
    logger = new LoggerProxy();

    TAG_LIST_EMPTY = ImmutableSet.of();

    TAG_LIST = ImmutableSet.of(
            Tag.builder().key("foo").value("bar").build()
    );

    TAG_LIST_ALTER = ImmutableSet.of(
            Tag.builder().key("bar").value("baz").build(),
            Tag.builder().key("fizz").value("buzz").build()
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

  static final String DB_SHARD_GROUP_IDENTIFIER = "testDbShardGroup";
  static final String DB_SHARD_GROUP_RESOURCE_ID = "testDbShardGroupId";
  static final String DB_CLUSTER_IDENTIFIER = "testDbCluster";

  static final String ACCOUNT_ID = "123456789012";
  static final String REGION = "us-east-1";
  static final String PARTITION = "aws";
  static final String DB_SHARD_GROUP_ARN = "arn:aws:rds:us-east-1:123456789012:shard-group:testDbShardGroupId";
  static final int COMPUTE_REDUNDANCY = 1;
  static final Double MAX_ACU = 3.5;
  static final Double MAX_ACU_ALTER = 5d;

  static final ResourceModel RESOURCE_MODEL = ResourceModel.builder()
          .dBShardGroupIdentifier(DB_SHARD_GROUP_IDENTIFIER)
          .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
          .dBShardGroupResourceId(DB_SHARD_GROUP_RESOURCE_ID)
          .computeRedundancy(COMPUTE_REDUNDANCY)
          .maxACU(MAX_ACU)
          .publiclyAccessible(false)
          .build();

  static final ResourceModel RESOURCE_MODEL_NO_IDENT = ResourceModel.builder()
          .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
          .dBShardGroupResourceId(DB_SHARD_GROUP_RESOURCE_ID)
          .computeRedundancy(COMPUTE_REDUNDANCY)
          .maxACU(MAX_ACU)
          .publiclyAccessible(false)
          .build();

  static final DBShardGroup DB_SHARD_GROUP = DBShardGroup.builder()
          .dbShardGroupIdentifier(DB_SHARD_GROUP_IDENTIFIER)
          .dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
          .dbShardGroupResourceId(DB_SHARD_GROUP_RESOURCE_ID)
          .computeRedundancy(COMPUTE_REDUNDANCY)
          .maxACU(MAX_ACU)
          .publiclyAccessible(false)
          .build();

  protected static final DBShardGroup DB_SHARD_GROUP_AVAILABLE = DB_SHARD_GROUP.toBuilder()
          .status(ResourceStatus.AVAILABLE.toString())
          .build();

  protected static final DBShardGroup DB_SHARD_GROUP_CREATING = DB_SHARD_GROUP.toBuilder()
          .status(ResourceStatus.CREATING.toString())
          .build();

  protected static final DBShardGroup DB_SHARD_GROUP_MODIFYING = DB_SHARD_GROUP.toBuilder()
          .status(ResourceStatus.MODIFYING.toString())
          .build();

  protected static final DBShardGroup DB_SHARD_GROUP_DELETING = DB_SHARD_GROUP.toBuilder()
          .status(ResourceStatus.DELETING.toString())
          .build();

  // use an accelerated backoff for faster unit testing
  protected final HandlerConfig TEST_HANDLER_CONFIG = HandlerConfig.builder()
          .probingEnabled(false)
          .backoff(Constant.of().delay(Duration.ofMillis(1))
                  .timeout(Duration.ofSeconds(120))
                  .build())
          .build();

  static <ClientT> ProxyClient<ClientT> MOCK_PROXY(final AmazonWebServicesClientProxy proxy, final ClientT client) {
    return new BaseProxyClient<>(proxy, client);
  }

  protected abstract BaseHandlerStd getHandler();

  protected abstract AmazonWebServicesClientProxy getProxy();

  protected abstract ProxyClient<RdsClient> getProxyClient();

  @Override
  protected String getLogicalResourceIdentifier() {
    return LOGICAL_RESOURCE_IDENTIFIER;
  }

  @Override
  protected String newClientRequestToken() {
    return CLIENT_REQUEST_TOKEN;
  }

  protected String newStackId() {
    return STACK_ID;
  }

  @Override
  protected void expectResourceSupply(Supplier<DBShardGroup> supplier) {
    when(getProxyClient().client().describeDBShardGroups(any(DescribeDbShardGroupsRequest.class)))
            .then((req) ->
                    DescribeDbShardGroupsResponse.builder()
                            .dbShardGroups(supplier.get())
                            .build());
  }

  @Override
  protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(ResourceHandlerRequest<ResourceModel> request, CallbackContext context) {
    return getHandler().handleRequest(getProxy(), getProxyClient(), request, context);
  }
}
