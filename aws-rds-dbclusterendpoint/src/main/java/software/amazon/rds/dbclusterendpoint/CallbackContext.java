package software.amazon.rds.dbclusterendpoint;

import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean createTagComplete;
    private String dbClusterEndpointArn;
}
