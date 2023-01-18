package software.amazon.rds.dbclustersnapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
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
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBClusterSnapshot, ResourceModel, CallbackContext> {

  protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbclustersnapshot";

  protected static final Credentials MOCK_CREDENTIALS;
  protected static final org.slf4j.Logger delegate;
  protected static final LoggerProxy logger;

  protected static final String DB_CLUSTER_IDENTIFIER = "db-cluster-identifier";
  protected static final String DB_CLUSTER_SNAPSHOT_ARN = "arn:aws:rds:us-east-1:608777835420:cluster-snapshot:db-cluster-snapshot-arn";
  protected static final String DB_CLUSTER_SNAPSHOT_IDENTIFIER = "db-cluster-snapshot-identifier";

  protected static final ResourceModel RESOURCE_MODEL;

  protected static final DBClusterSnapshot DB_CLUSTER_SNAPSHOT_ACTIVE;

  protected static final String ERROR_MSG = "error";

  protected static final List<Tag> TAG_LIST;
  protected static final List<Tag> TAG_LIST_EMPTY;
  protected static final Set<Tag> TAG_LIST_ALTER;
  protected static final Tagging.TagSet TAG_SET;

  protected static Constant TEST_BACKOFF_DELAY = Constant.of()
          .delay(Duration.ofSeconds(1L))
          .timeout(Duration.ofSeconds(10L))
          .build();

  static {
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
    MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

    delegate = LoggerFactory.getLogger("testing");
    logger = new LoggerProxy();

    RESOURCE_MODEL = ResourceModel.builder()
            .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
            .dBClusterSnapshotArn(DB_CLUSTER_SNAPSHOT_ARN)
            .dBClusterSnapshotIdentifier(DB_CLUSTER_SNAPSHOT_IDENTIFIER)
            .build();

    DB_CLUSTER_SNAPSHOT_ACTIVE = DBClusterSnapshot.builder()
            .dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
            .dbClusterSnapshotArn(DB_CLUSTER_SNAPSHOT_ARN)
            .dbClusterSnapshotIdentifier(DB_CLUSTER_SNAPSHOT_IDENTIFIER)
            .status("available")
            .build();

    TAG_LIST_EMPTY = Collections.emptyList();

    TAG_LIST = Arrays.asList(
            Tag.builder().key("foo-1").value("bar-1").build(),
            Tag.builder().key("foo-2").value("bar-2").build(),
            Tag.builder().key("foo-3").value("bar-3").build()
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

  static <ClientT> ProxyClient<ClientT> MOCK_PROXY(final AmazonWebServicesClientProxy proxy, final ClientT client) {
    return new BaseProxyClient<>(proxy, client);
  }

  protected abstract BaseHandlerStd getHandler();

  protected abstract AmazonWebServicesClientProxy getProxy();

  protected abstract ProxyClient<RdsClient> getProxyClient();

  public abstract HandlerName getHandlerName();


  private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

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
    return getHandler().handleRequest(getProxy(), request, context, getProxyClient(), logger);
  }

  @Override
  protected String getLogicalResourceIdentifier() {
    return LOGICAL_RESOURCE_IDENTIFIER;
  }

  @Override
  protected void expectResourceSupply(final Supplier<DBClusterSnapshot> supplier) {
    when(getProxyClient()
            .client()
            .describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class))
    ).then(res -> DescribeDbClusterSnapshotsResponse.builder()
            .dbClusterSnapshots(supplier.get())
            .build()
    );
  }
}
