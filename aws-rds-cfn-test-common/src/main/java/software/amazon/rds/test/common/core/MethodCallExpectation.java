package software.amazon.rds.test.common.core;

import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;

public interface MethodCallExpectation<RequestT extends AwsRequest, ResponseT extends AwsResponse> {
    OngoingStubbing<ResponseT> setup();

    ArgumentCaptor<RequestT> verify();
}
