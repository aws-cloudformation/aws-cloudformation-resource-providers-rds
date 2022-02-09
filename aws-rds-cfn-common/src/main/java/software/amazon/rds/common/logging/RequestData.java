package software.amazon.rds.common.logging;

import java.util.Map;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@Data
@AllArgsConstructor
class RequestData {
    private final Map<String, String> requestDataMap = Maps.newHashMap();

    public <T> RequestData(final ResourceHandlerRequest<T> request) {
        requestDataMap.put("StackId", request.getStackId());
        requestDataMap.put("AwsAccountId", request.getAwsAccountId());
        requestDataMap.put("ClientRequestToken", request.getClientRequestToken());
    }

}
