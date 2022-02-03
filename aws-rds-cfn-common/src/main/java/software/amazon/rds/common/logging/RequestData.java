package software.amazon.rds.common.logging;

import org.json.JSONObject;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class RequestData {
    private String stackId;
    private String awsAccountId;
    private String clientRequestToken;

    public JSONObject toJson() {
        JSONObject requestDataJsonObject = new JSONObject();
        requestDataJsonObject.put("StackId", stackId);
        requestDataJsonObject.put("AwsAccountId", awsAccountId);
        requestDataJsonObject.put("ClientRequestToken", clientRequestToken);
        return requestDataJsonObject;
    }

}
