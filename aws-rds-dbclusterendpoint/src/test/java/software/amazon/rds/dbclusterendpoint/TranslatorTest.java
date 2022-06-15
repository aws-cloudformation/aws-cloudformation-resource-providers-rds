package software.amazon.rds.dbclusterendpoint;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;

import static org.assertj.core.api.Assertions.assertThat;

public class TranslatorTest {

    @Test
    public void describeDbClusterEndpointRequest_SetsDbClusterEndpointIdentifier() {
        final ResourceModel model = ResourceModel.builder().dBClusterEndpointIdentifier("foo").build();

        final DescribeDbClusterEndpointsRequest describeDbClusterEndpointsRequest = Translator.describeDbClustersEndpointRequest(model);

        assertThat(describeDbClusterEndpointsRequest.dbClusterEndpointIdentifier()).isEqualTo("foo");
    }

    @Test
    public void describeDbClusterEndpointRequest_SetsMarker() {
        final DescribeDbClusterEndpointsRequest describeDbClusterEndpointsRequest = Translator.describeDbClustersEndpointRequest("marker");

        assertThat(describeDbClusterEndpointsRequest.marker()).isEqualTo("marker");
    }

    @Test
    public void deleteDbClusterEndpointRequest_SetsDbClusterEndpointIdentifier() {
        final ResourceModel model = ResourceModel.builder().dBClusterEndpointIdentifier("foo").build();

        final DeleteDbClusterEndpointRequest deleteDbClusterEndpointRequest = Translator.deleteDbClusterEndpointRequest(model);

        assertThat(deleteDbClusterEndpointRequest.dbClusterEndpointIdentifier()).isEqualTo("foo");
    }

    @Test void translateDbClusterEndpointFromSdk_SetsAllFields() {
        final DBClusterEndpoint dbClusterEndpoint = DBClusterEndpoint.builder()
                .dbClusterEndpointIdentifier("cluster-endpoint-identifier")
                .dbClusterIdentifier("cluster-identifier")
                .customEndpointType("READER")
                .dbClusterEndpointArn("cluster-endpoint-arn")
                .staticMembers("static-member")
                .endpoint("endpoint-address")
                .build();

        final ResourceModel resourceModel = Translator.translateDbClusterEndpointFromSdk(dbClusterEndpoint);
        assertThat(resourceModel.getDBClusterEndpointIdentifier()).isEqualTo("cluster-endpoint-identifier");
        assertThat(resourceModel.getDBClusterIdentifier()).isEqualTo("cluster-identifier");
        assertThat(resourceModel.getEndpointType()).isEqualTo("READER");
        assertThat(resourceModel.getDBClusterEndpointArn()).isEqualTo("cluster-endpoint-arn");
        assertThat(resourceModel.getStaticMembers()).containsExactly("static-member");
        assertThat(resourceModel.getEndpoint()).isEqualTo("endpoint-address");
    }
}
