package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterEndpointRequest;
import software.amazon.rds.common.handler.Tagging;

import java.util.HashSet;
import java.util.Map;

public class Translator {

    static CreateDbClusterEndpointRequest createDbClusterEndpointRequest(
            final ResourceModel model,
            final Map<String, String> tags
    ) {
        return CreateDbClusterEndpointRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .endpointType(model.getEndpointType())
                .staticMembers(model.getStaticMembers())
                .excludedMembers(model.getExcludedMembers())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbClusterEndpointsRequest describeDbClustersEndpointRequest(final ResourceModel model) {
        return DescribeDbClusterEndpointsRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .build();
    }

    static DescribeDbClusterEndpointsRequest describeDbClustersEndpointRequest(final String nextToken) {
        return DescribeDbClusterEndpointsRequest.builder()
                .marker(nextToken)
                .build();
    }


    static ResourceModel translateDbClusterEndpointFromSdk(
            final software.amazon.awssdk.services.rds.model.DBClusterEndpoint dbClusterEndpoint) {
        return ResourceModel.builder()
                .dBClusterEndpointIdentifier(dbClusterEndpoint.dbClusterEndpointIdentifier())
                .dBClusterIdentifier(dbClusterEndpoint.dbClusterIdentifier())
                .endpointType(dbClusterEndpoint.customEndpointType())
                .staticMembers(new HashSet<>(dbClusterEndpoint.staticMembers()))
                .excludedMembers(new HashSet<>(dbClusterEndpoint.excludedMembers()))
                .build();
    }

    static DeleteDbClusterEndpointRequest deleteDbClusterEndpointRequest(final ResourceModel model) {
        return DeleteDbClusterEndpointRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .build();
    }

    static ModifyDbClusterEndpointRequest modifyDbClusterEndpoint(final ResourceModel model) {
        return ModifyDbClusterEndpointRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .endpointType(model.getEndpointType())
                .staticMembers(model.getStaticMembers())
                .excludedMembers(model.getExcludedMembers())
                .build();
    }
}
