package software.amazon.rds.dbinstance.common;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.Translator;

@AllArgsConstructor
public class Fetch {
    private final ProxyClient<RdsClient> rdsProxyClient;

    public DBInstance dbInstance(final ResourceModel model) {
        final DescribeDbInstancesResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbInstancesRequest(model),
                rdsProxyClient.client()::describeDBInstances
        );
        return response.dbInstances().get(0);
    }

    public DBSnapshot dbSnapshot(final ResourceModel model) {
        final DescribeDbSnapshotsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                Translator.describeDbSnapshotsRequest(model),
                rdsProxyClient.client()::describeDBSnapshots
        );
        return response.dbSnapshots().get(0);
    }
}
