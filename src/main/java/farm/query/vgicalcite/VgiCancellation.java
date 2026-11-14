package farm.query.vgicalcite;

import org.apache.calcite.DataContext;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Observes Calcite cancellation while the scan thread is blocked in transport I/O. */
final class VgiCancellation implements AutoCloseable {
    private static final ScheduledThreadPoolExecutor WATCHER = watcher();
    private final AtomicBoolean flag;
    private final long deadline;
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private final ScheduledFuture<?> watch;
    private volatile Runnable abort;
    private volatile Thread activeThread;
    private volatile boolean closed;

    VgiCancellation(DataContext context) {
        flag = DataContext.Variable.CANCEL_FLAG.get(context);
        Long timeout = DataContext.Variable.TIMEOUT.get(context);
        Long started = DataContext.Variable.UTC_TIMESTAMP.get(context);
        if (timeout != null && timeout > 0) {
            long elapsed = started == null ? 0 : Math.max(0, System.currentTimeMillis() - started);
            deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeout - elapsed));
        } else {
            deadline = 0;
        }
        watch = WATCHER.scheduleWithFixedDelay(this::poll, 25, 25, TimeUnit.MILLISECONDS);
    }

    /** The action must be idempotent and must not wait for the scan thread or worker. */
    void onAbort(Runnable action) {
        abort = action;
        if (failure.get() != null) action.run();
    }

    void enter() { activeThread = Thread.currentThread(); check(); }
    void exit() { activeThread = null; }
    RuntimeException failure() { return failure.get(); }

    void check() {
        poll();
        RuntimeException stopped = failure.get();
        if (stopped != null) throw stopped;
    }

    private void poll() {
        if (closed || failure.get() != null) return;
        Thread active = activeThread;
        RuntimeException stopped;
        if (flag != null && flag.get() || active != null && active.isInterrupted()) {
            stopped = new CancellationException("VGI query cancelled");
        } else if (deadline != 0 && System.nanoTime() - deadline >= 0) {
            stopped = new IllegalStateException("VGI query timed out",
                    new java.sql.SQLTimeoutException("VGI query timed out"));
        } else {
            return;
        }
        if (failure.compareAndSet(null, stopped)) {
            Runnable action = abort;
            if (action != null) {
                try { action.run(); } catch (RuntimeException cleanup) { stopped.addSuppressed(cleanup); }
            }
        }
    }

    @Override public void close() {
        closed = true;
        watch.cancel(false);
        abort = null;
        activeThread = null;
    }

    private static ScheduledThreadPoolExecutor watcher() {
        var executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "vgi-calcite-cancellation");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
