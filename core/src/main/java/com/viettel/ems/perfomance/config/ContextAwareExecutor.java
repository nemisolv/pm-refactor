package com.viettel.ems.perfomance.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class ContextAwareExecutor {

    private final ExecutorService delegate;
    private final SystemType systemType;
    private final String dbAlias;
    @Getter
    private final ScheduledExecutorService scheduledExecutorService;

    public ContextAwareExecutor(ExecutorService delegate, SystemType systemType, String dbAlias) {
        this.delegate = delegate;
        this.systemType = systemType;
        this.dbAlias = dbAlias;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(15);
        TenantContextHolder.setCurrentDatasourceKey(dbAlias);
        TenantContextHolder.setCurrentSystem(systemType);
    }

    public void execute(Runnable task) {
        delegate.execute(wrapRunnable(task));
    }

    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(wrapCallable(task));
    }

    public Future<?> submit(Runnable task) {
        return delegate.submit(wrapRunnable(task));
    }

    private Runnable wrapRunnable(Runnable original) {
        return () -> {
            TenantContextHolder.setCurrentSystem(systemType);
            TenantContextHolder.setCurrentDatasourceKey(dbAlias);
            final String previousName = Thread.currentThread().getName();
            final String renamed = String.format("SYS-%s-%s-%s", systemType.getCode(), dbAlias, previousName);
            Thread.currentThread().setName(renamed);
            try {
                original.run();
            } finally {
                Thread.currentThread().setName(previousName);
                TenantContextHolder.clear();
            }
        };
    }

    private <T> Callable<T> wrapCallable(Callable<T> original) {
        return () -> {
            TenantContextHolder.setCurrentSystem(systemType);
            TenantContextHolder.setCurrentDatasourceKey(dbAlias);
            final String previousName = Thread.currentThread().getName();
            final String renamed = String.format("SYS-%s-%s-%s", systemType.getCode(), dbAlias, previousName);
            Thread.currentThread().setName(renamed);
            try {
                return original.call();
            } finally {
                Thread.currentThread().setName(previousName);
                TenantContextHolder.clear();
            }
        };
    }

    public void shutdown() {
        delegate.shutdown();
    }

    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

}