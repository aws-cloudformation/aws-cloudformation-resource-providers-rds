package software.amazon.rds.dbinstance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import java.util.function.Supplier;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public abstract class AbstractHandlerTest extends AbstractTestBase {

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    protected abstract ProxyClient<Ec2Client> getEc2Proxy();

    protected ProgressEvent<ResourceModel, CallbackContext> test_handleRequest_base(
            final CallbackContext context,
            final Supplier<DBInstance> dbInstanceSupplier,
            final Supplier<ResourceModel> desiredStateSupplier,
            final Consumer<ProgressEvent<ResourceModel, CallbackContext>> expect
    ) {
        return test_handleRequest_base(context, dbInstanceSupplier, null, desiredStateSupplier, expect);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> test_handleRequest_base(
            final CallbackContext context,
            final Supplier<DBInstance> dbInstanceSupplier,
            final Supplier<ResourceModel> previousStateSupplier,
            final Supplier<ResourceModel> desiredStateSupplier,
            final Consumer<ProgressEvent<ResourceModel, CallbackContext>> expect
    ) {
        return test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder(),
                dbInstanceSupplier,
                previousStateSupplier,
                desiredStateSupplier,
                expect
        );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> test_handleRequest_base(
            final CallbackContext context,
            final ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> builder,
            final Supplier<DBInstance> dbInstanceSupplier,
            final Supplier<ResourceModel> previousStateSupplier,
            final Supplier<ResourceModel> desiredStateSupplier,
            final Consumer<ProgressEvent<ResourceModel, CallbackContext>> expect
    ) {
        if (dbInstanceSupplier != null) {
            when(getRdsProxy().client().describeDBInstances(any(DescribeDbInstancesRequest.class))).then(res -> {
                return DescribeDbInstancesResponse.builder().dbInstances(dbInstanceSupplier.get()).build();
            });
        }

        builder.desiredResourceState(desiredStateSupplier.get());
        if (previousStateSupplier != null) {
            builder.previousResourceState(previousStateSupplier.get());
        }
        builder.logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER);
        builder.clientRequestToken(getClientRequestToken());
        builder.stackId(getStackId());

        final ProgressEvent<ResourceModel, CallbackContext> response = getHandler().handleRequest(
                getProxy(),
                builder.build(),
                context,
                getRdsProxy(),
                getEc2Proxy(),
                logger
        );
        expect.accept(response);

        return response;
    }

}
