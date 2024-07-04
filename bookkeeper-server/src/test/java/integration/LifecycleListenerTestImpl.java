package integration;

import org.apache.bookkeeper.common.component.LifecycleListener;

public class LifecycleListenerTestImpl implements LifecycleListener {

    private final StringBuilder stringBuilder;

    public LifecycleListenerTestImpl() {
        stringBuilder = new StringBuilder();
    }

    public String getLog() {
        return stringBuilder.toString();
    }

    @Override
    public void beforeStart() {
        stringBuilder.append("This is the method beforeStart\n");
    }

    @Override
    public void afterStart() {
        stringBuilder.append("This is the method afterStart\n");
    }

    @Override
    public void beforeStop() {
        stringBuilder.append("This is the method beforeStop\n");
    }

    @Override
    public void afterStop() {
        stringBuilder.append("This is the method afterStop\n");
    }

    @Override
    public void beforeClose() {
        stringBuilder.append("This is the method beforeClose\n");
    }

    @Override
    public void afterClose() {
        stringBuilder.append("This is the method afterClose\n");
    }
}