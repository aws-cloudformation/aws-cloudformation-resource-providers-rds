package software.amazon.rds.dbclusterendpoint;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;

import static org.assertj.core.api.Assertions.assertThat;


public class TranslatorTest {

    @Test
    public void describeDbClusterEndpointRequest_setsDbClusterEndpointIdentifier() {
        final ResourceModel model = ResourceModel.builder().dBClusterEndpointIdentifier("foo").build();

        DescribeDbClusterEndpointsRequest describeDbClusterEndpointsRequest = Translator.describeDbClustersEndpointRequest(model);

        assertThat(describeDbClusterEndpointsRequest.dbClusterEndpointIdentifier()).isEqualTo("foo");
    }

    @Test
    public void describeDbClusterEndpointRequest_setsMarker() {
        final DescribeDbClusterEndpointsRequest describeDbClusterEndpointsRequest = Translator.describeDbClustersEndpointRequest("marker");

        assertThat(describeDbClusterEndpointsRequest.marker()).isEqualTo("marker");
    }
}