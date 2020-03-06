package software.amazon.rds.dbclusterparametergroup;

import software.amazon.cloudformation.proxy.StdCallbackContext;


@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean parametersApplied;
}
