import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CustomThreadPool implements CustomExecutor {

    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());
    private final String poolName;  // Для уникального именования
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads; // Минимальное количество всегда активных потоков

    private final BlockingQueue<Runnable>[] queues;
    private final Worker[] workers;
    private final CustomThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private volatile boolean isShutdown = false;
    private final AtomicInteger activeThreads = new AtomicInteger(0); // Счетчик активных потоков

    public CustomThreadPool(String poolName, int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit, int queueSize, int minSpareThreads, RejectedExecutionHandler rejectedExecutionHandler) {
        if (corePoolSize <= 0 || maxPoolSize <= 0 || keepAliveTime <= 0 || queueSize <= 0 || minSpareThreads < 0) {
            throw new IllegalArgumentException("Invalid pool parameters.");
        }
        if (corePoolSize > maxPoolSize) {
            throw new IllegalArgumentException("corePoolSize cannot be greater than maxPoolSize");
        }
        if (minSpareThreads > corePoolSize) {
            throw new IllegalArgumentException("minSpareThreads cannot be greater than corePoolSize");
        }

        this.poolName = (poolName != null && !poolName.trim().isEmpty()) ? poolName : "MyPool"; // Если имя не задано, устанавливаем дефолтное
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        this.queues = new BlockingQueue[corePoolSize];
        for (int i = 0; i < corePoolSize; i++) {
            queues[i] = new LinkedBlockingQueue<>(queueSize);
        }

        this.workers = new Worker[maxPoolSize];  // Увеличиваем массив для maxPoolSize
        this.threadFactory = new CustomThreadFactory(poolName);
        this.rejectedExecutionHandler = (rejectedExecutionHandler != null) ? rejectedExecutionHandler : new CallerRunsPolicy(); // Используем переданный обработчик или политику по умолчанию

        initializeCoreThreads();
    }

    private void initializeCoreThreads() {
        for (int i = 0; i < corePoolSize; i++) {
            startWorker(i);
        }
        ensureMinSpareThreads(); // Гарантируем наличие minSpareThreads
    }


    // Запуск worker'а (потока)
    private void startWorker(int queueIndex) {
        if (isShutdown) {
            return;
        }

        if(workers[queueIndex] != null) {
            return; // Поток уже запущен в этом слоте
        }

        Worker worker = new Worker(queueIndex);
        Thread thread = threadFactory.newThread(worker);
        workers[queueIndex] = worker;
        thread.start();
        activeThreads.incrementAndGet();
    }

    // Гарантируем, что всегда есть minSpareThreads
    private void ensureMinSpareThreads() {
        int currentActive = activeThreads.get();
        if (currentActive < minSpareThreads) {
            for (int i = 0; i < corePoolSize; i++) { // Iterate corePoolSize, not maxPoolSize
                if(workers[i] == null) {
                    startWorker(i);
                }
            }
        }
    }

    @Override
    public void execute(Runnable task) {
        if (isShutdown) {
            rejectedExecutionHandler.rejectedExecution(task, this);
            return;
        }

        // Round Robin
        int queueIndex = threadCounter.getAndIncrement() % corePoolSize;
        BlockingQueue<Runnable> queue = queues[queueIndex];
        String taskDescription = task.getClass().getName();

        logger.log(Level.INFO, String.format("[Pool] Task accepted into queue #%d: %s", queueIndex, taskDescription));

        try {
            if (!queue.offer(task)) {
                // Очередь переполнена.  Обрабатываем согласно политике
                logger.log(Level.WARNING, String.format("[Rejected] Task %s was rejected due to overload!", taskDescription));
                rejectedExecutionHandler.rejectedExecution(task, this);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("[Rejected] Task %s was rejected due to exception: %s", taskDescription, e.getMessage()), e);
            rejectedExecutionHandler.rejectedExecution(task, this);
        }

        //  Добавляем новые потоки, если нужно
        if(activeThreads.get() < corePoolSize &&  queue.size() > queueSize/2) {
            ensureMinSpareThreads();
        }
        if(activeThreads.get() < maxPoolSize && queue.size() > queueSize ) {
            int nextWorker = -1;
            for(int i = 0; i < maxPoolSize; ++i){
                if(workers[i] == null){
                    nextWorker = i;
                    break;
                }
            }
            if(nextWorker != -1) {
                startWorker(nextWorker);
            }
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException();
        }
        RunnableFuture<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        for (Worker worker : workers) {
            if (worker != null) {
                worker.interrupt(); // Прерываем потоки
            }
        }
        logger.log(Level.INFO, "[Pool] Shutdown initiated.");
    }

    @Override
    public void shutdownNow() {
        shutdown();
        // Попытка отменить все оставшиеся задачи
        for (BlockingQueue<Runnable> queue : queues) {
            for (Runnable task : queue.toArray(new Runnable[0])) {
                if (task instanceof FutureTask) {
                    ((FutureTask<?>) task).cancel(true); // прерываем задачу
                }
            }
        }
        logger.log(Level.INFO, "[Pool] shutdownNow() called. Attempting to cancel running tasks.");
    }


    public boolean isShutdown() {
        return isShutdown;
    }

    // ThreadFactory
    private static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String poolName;

        public CustomThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = poolName + "-worker-" + threadNumber.getAndIncrement();
            Thread thread = new Thread(r, threadName);
            logger.log(Level.INFO, "[ThreadFactory] Creating new thread: " + threadName);
            thread.setDaemon(false);  // Потоки не должны быть демонами
            return thread;
        }
    }

    // RejectedExecutionHandler (Политика отклонения)
    public static class CallerRunsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPool executor) {
            if (!executor.isShutdown()) { // Проверяем, не был ли пул завершен
                String taskDescription = r.getClass().getName();
                logger.log(Level.WARNING, String.format("[Rejected] Task %s was rejected due to overload, executing in caller's thread!", taskDescription));
                r.run(); // Выполняем задачу в потоке вызывающего
            } else {
                String taskDescription = r.getClass().getName();
                logger.log(Level.WARNING, String.format("[Rejected] Task %s was rejected, pool is shutting down!", taskDescription));
            }
        }
    }

    // Worker (рабочий поток)
    private class Worker implements Runnable {
        private final int queueIndex;
        private volatile boolean isRunning = true;

        public Worker(int queueIndex) {
            this.queueIndex = queueIndex;
        }

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            try {
                while (isRunning && !isShutdown) {
                    Runnable task = queues[queueIndex].poll(keepAliveTime, timeUnit);
                    if (task != null) {
                        try {
                            String taskDescription = task.getClass().getName();
                            logger.log(Level.INFO, String.format("[Worker] %s executes %s", currentThread.getName(), taskDescription));
                            if (!isShutdown) {
                                task.run();
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Exception during task execution", e);
                        }
                    } else {
                        // Таймаут: поток простаивал
                        if (activeThreads.get() > corePoolSize) {
                            // Завершаем поток, если их больше corePoolSize
                            logger.log(Level.INFO, String.format("[Worker] %s idle timeout, stopping.", currentThread.getName()));
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                // Прерывание во время ожидания задачи.  Завершаем поток.
                logger.log(Level.INFO, String.format("[Worker] %s interrupted, exiting.", currentThread.getName()));
            } finally {
                isRunning = false;
                workers[queueIndex] = null;
                activeThreads.decrementAndGet();
                logger.log(Level.INFO, String.format("[Worker] %s terminated.", currentThread.getName()));
            }
        }

        public void interrupt() {
            isRunning = false;
            Thread.currentThread().interrupt();
        }
    }

    // Методы для тестирования и мониторинга (необязательно, но полезно)
    public int getActiveThreadCount() {
        return activeThreads.get();
    }

    public int getQueueSize(int queueIndex) {
        return queues[queueIndex].size();
    }
}
