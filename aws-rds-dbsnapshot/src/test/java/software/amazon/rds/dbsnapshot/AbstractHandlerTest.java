package software.amazon.rds.dbsnapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.MethodCallExpectation;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBSnapshot, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbsnapshot";
    protected static final LoggerProxy logger = new LoggerProxy();
    protected static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();
    protected static final Credentials MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
    protected static final String ERROR_MSG = "error";

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofMillis(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    protected abstract HandlerName getHandlerName();

    protected abstract BaseHandlerStd getHandler();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    @Override
    protected void expectResourceSupply(Supplier<DBSnapshot> supplier) {
        expectDescribeDBSnapshotsCall().setup().then(res -> DescribeDbSnapshotsResponse.builder()
                .dbSnapshots(supplier.get())
                .build());
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(ResourceHandlerRequest<ResourceModel> request, CallbackContext context) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), logger);
    }

    protected MethodCallExpectation<DescribeDbSnapshotsRequest, DescribeDbSnapshotsResponse> expectDescribeDBSnapshotsCall() {
        return new MethodCallExpectation<DescribeDbSnapshotsRequest, DescribeDbSnapshotsResponse>() {
            @Override
            public OngoingStubbing<DescribeDbSnapshotsResponse> setup() {
                return when(getRdsProxy().client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)));
            }

            @Override
            public ArgumentCaptor<DescribeDbSnapshotsRequest> verify() {
                ArgumentCaptor<DescribeDbSnapshotsRequest> captor = ArgumentCaptor.forClass(DescribeDbSnapshotsRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).describeDBSnapshots(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<CreateDbSnapshotRequest, CreateDbSnapshotResponse> expectCreateDBSnapshotCall() {
        return new MethodCallExpectation<CreateDbSnapshotRequest, CreateDbSnapshotResponse>() {

            @Override
            public OngoingStubbing<CreateDbSnapshotResponse> setup() {
                return when(getRdsProxy().client().createDBSnapshot(any(CreateDbSnapshotRequest.class)));
            }

            @Override
            public ArgumentCaptor<CreateDbSnapshotRequest> verify() {
                ArgumentCaptor<CreateDbSnapshotRequest> captor = ArgumentCaptor.forClass(CreateDbSnapshotRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).createDBSnapshot(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<ModifyDbSnapshotRequest, ModifyDbSnapshotResponse> expectModifyDBSnapshotCall() {
        return new MethodCallExpectation<ModifyDbSnapshotRequest, ModifyDbSnapshotResponse>() {

            @Override
            public OngoingStubbing<ModifyDbSnapshotResponse> setup() {
                return when(getRdsProxy().client().modifyDBSnapshot(any(ModifyDbSnapshotRequest.class)));
            }

            @Override
            public ArgumentCaptor<ModifyDbSnapshotRequest> verify() {
                ArgumentCaptor<ModifyDbSnapshotRequest> captor = ArgumentCaptor.forClass(ModifyDbSnapshotRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).modifyDBSnapshot(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<DeleteDbSnapshotRequest, DeleteDbSnapshotResponse> expectDeleteDBSnapshotCall() {
        return new MethodCallExpectation<DeleteDbSnapshotRequest, DeleteDbSnapshotResponse>() {

            @Override
            public OngoingStubbing<DeleteDbSnapshotResponse> setup() {
                return when(getRdsProxy().client().deleteDBSnapshot(any(DeleteDbSnapshotRequest.class)));
            }

            @Override
            public ArgumentCaptor<DeleteDbSnapshotRequest> verify() {
                ArgumentCaptor<DeleteDbSnapshotRequest> captor = ArgumentCaptor.forClass(DeleteDbSnapshotRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).deleteDBSnapshot(captor.capture());
                return captor;
            }
        };
    }
}
