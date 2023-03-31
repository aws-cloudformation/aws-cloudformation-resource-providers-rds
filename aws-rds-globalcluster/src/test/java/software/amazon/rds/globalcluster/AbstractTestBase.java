package software.amazon.rds.globalcluster;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;

import org.slf4j.LoggerFactory;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {

  protected static final Credentials MOCK_CREDENTIALS;
  protected static final org.slf4j.Logger delegate;
  protected static final LoggerProxy logger;


  protected static final Integer BACKUP_RETENTION_PERIOD;
  protected static final Integer BACKTRACK_WINDOW;
  protected static final String GLOBALCLUSTER_IDENTIFIER;
  protected static final String SOURCECLUSTER_IDENTIFIER;
  protected static final String SOURCECLUSTER_ARN;
  protected static final String ENGINE_VERSION;
  protected static final String ENGINE_VERSION_MVU;
  protected static final String ENGINE;
  protected static final boolean DELETION_PROTECTION;

  protected static final ResourceModel RESOURCE_MODEL;
  protected static final ResourceModel RESOURCE_MODEL_UPDATE;
  protected static final ResourceModel RESOURCE_MODEL_UPDATE_MVU;
  protected static final ResourceModel RESOURCE_MODEL_ALTERNATIVE;
  protected static final ResourceModel RESOURCE_MODEL_WITH_MASTER;
  protected static final ResourceModel RESOURCE_MODEL_WITH_MASTER_ARN;
  protected static final ResourceModel RESOURCE_MODEL_EMPTY_IDENTIFIER;

  protected static final GlobalCluster GLOBAL_CLUSTER_ACTIVE;
  protected static final DBCluster DBCLUSTER_ACTIVE;

  static {
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
    MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

    delegate = LoggerFactory.getLogger("testing");
    logger = new LoggerProxy();

    BACKUP_RETENTION_PERIOD = 1;
    BACKTRACK_WINDOW = 1;
    GLOBALCLUSTER_IDENTIFIER = "my-sample-globalcluster";
    SOURCECLUSTER_IDENTIFIER = "my-sample-dbcluster";
    SOURCECLUSTER_ARN = "arn:aws:rds:us-east-1:340834135580:cluster:sample-globalcluster";
    ENGINE = "aurora-mysql";
    ENGINE_VERSION = "5.7.mysql_aurora.2.11.1";
    ENGINE_VERSION_MVU = "8.0.mysql_aurora.3.03.0";
    DELETION_PROTECTION = false;


    RESOURCE_MODEL = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .engineVersion(ENGINE_VERSION)
            .engine(ENGINE)
            .build();

    RESOURCE_MODEL_ALTERNATIVE = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .engineVersion(ENGINE_VERSION)
            .engine(ENGINE)
            .build();

    RESOURCE_MODEL_WITH_MASTER = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .sourceDBClusterIdentifier(SOURCECLUSTER_IDENTIFIER)
            .engineVersion(ENGINE_VERSION)
            .engine(ENGINE)
            .build();

    RESOURCE_MODEL_WITH_MASTER_ARN = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .sourceDBClusterIdentifier(SOURCECLUSTER_ARN)
            .engineVersion(ENGINE_VERSION)
            .engine(ENGINE)
            .build();

    RESOURCE_MODEL_EMPTY_IDENTIFIER = ResourceModel.builder()
            .globalClusterIdentifier(null)
            .engine(ENGINE)
            .build();

    GLOBAL_CLUSTER_ACTIVE = GlobalCluster.builder()
            .globalClusterIdentifier(RESOURCE_MODEL.getGlobalClusterIdentifier())
            .deletionProtection(false)
            .engine(RESOURCE_MODEL.getEngine())
            .engineVersion(RESOURCE_MODEL.getEngineVersion())
            .status(GlobalClusterStatus.Available.toString())
            .build();

    DBCLUSTER_ACTIVE = DBCluster.builder()
            .dbClusterIdentifier(RESOURCE_MODEL_WITH_MASTER.getSourceDBClusterIdentifier())
            .dbClusterArn("123")
            .build();

    RESOURCE_MODEL_UPDATE = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .deletionProtection(DELETION_PROTECTION)
            .engineVersion(ENGINE_VERSION)
            .build();

    RESOURCE_MODEL_UPDATE_MVU = ResourceModel.builder()
            .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
            .deletionProtection(DELETION_PROTECTION)
            .engineVersion(ENGINE_VERSION_MVU)
            .build();
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
}
