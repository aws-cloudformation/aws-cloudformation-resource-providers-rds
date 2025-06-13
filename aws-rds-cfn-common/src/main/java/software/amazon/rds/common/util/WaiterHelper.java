package software.amazon.rds.common.util;

import software.amazon.cloudformation.proxy.ProgressEvent;

public class WaiterHelper {
    /**
     * A function which introduces artificial delay during any ProgressEvent, usually used in Aurora codepaths, for example
     * there might be asynchronous workflows running to update resources after the resource has become available and which
     * we don't have a valid status to stabilise on
     *
     * This function will wait for callbackDelay, return the progress event and allow the handler
     * to be reinvoke itself and keep retrying until the maxTimeSeconds is breached
     *
     * @param evt ProgressEvent of the handler
     * @param maxSeconds The max total time we will wait
     * @param pollSeconds Wait time before the next invocation of the handler, until we reach maxSeconds
     * @return ProgressEvent
     * @param <ResourceT> The generic resource model
     * @param <CallbackT> The generic callback
     */
    public static <ResourceT, CallbackT extends DelayContext> ProgressEvent<ResourceT, CallbackT> delay(final ProgressEvent<ResourceT, CallbackT> evt, final int maxSeconds, final int pollSeconds) {
        final CallbackT callbackContext = evt.getCallbackContext();
        if (shouldDelay(callbackContext, maxSeconds)) {
            callbackContext.setWaitTime(callbackContext.getWaitTime() + pollSeconds);
            return ProgressEvent.defaultInProgressHandler(callbackContext, pollSeconds, evt.getResourceModel());
        } else {
            return ProgressEvent.progress(evt.getResourceModel(), callbackContext);
        }
    }

    public static <CallbackT extends DelayContext> boolean shouldDelay(CallbackT callbackContext, int maxSeconds) {
        return callbackContext.getWaitTime() <= maxSeconds;
    }

    public interface DelayContext {
        int getWaitTime();
        void setWaitTime(int waitTime);
    }
}
