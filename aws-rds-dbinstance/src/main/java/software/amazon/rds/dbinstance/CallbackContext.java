package software.amazon.rds.dbinstance;

import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean modified;
    private boolean deleting;
    private boolean rolesUpdated;
    private boolean updatedAfterCreate;
}
