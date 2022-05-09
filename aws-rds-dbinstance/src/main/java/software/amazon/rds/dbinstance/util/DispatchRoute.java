package software.amazon.rds.dbinstance.util;

import java.util.function.Predicate;

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.dbinstance.ResourceModel;

@AllArgsConstructor
public class DispatchRoute<H> {
    @Getter
    private final Predicate<ResourceHandlerRequest<ResourceModel>> predicate;

    @Getter
    private final H handler;
}
