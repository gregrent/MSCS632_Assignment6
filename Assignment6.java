import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

public class Assignment6 {
    private static final Logger logger = Logger.getLogger("RideShareLogger");
    private final Queue<String> taskQueue = new LinkedList<>();
    private final List<String> results = new ArrayList<>();
    private final Lock queueLock = new ReentrantLock();
    private final Lock resultsLock = new ReentrantLock();
    private final Condition notEmpty = queueLock.newCondition();

    public void addTask(String task) {
        queueLock.lock();
        try {
            taskQueue.add(task);
            notEmpty.signal();
        } finally {
            queueLock.unlock();
        }
    }

    public String getTask() throws InterruptedException {
        queueLock.lock();
        try {
            while (taskQueue.isEmpty()) {
                notEmpty.await();
            }
            return taskQueue.poll();
        } finally {
            queueLock.unlock();
        }
    }

    public void worker(int id) {
        Thread.currentThread().setName("Worker-" + id);
        logger.info(Thread.currentThread().getName() + " started.");
        while (true) {
            String task;
            try {
                task = getTask();
            } catch (InterruptedException ie) {
                logger.warning(Thread.currentThread().getName() + " interrupted. Exiting.");
                break;
            }
            try {
                Thread.sleep(new Random().nextInt(500) + 500); // simulate compute
                String result = Thread.currentThread().getName() + " processed: " + task;
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
                logger.info(result);
            } catch (InterruptedException ie) {
                logger.warning(Thread.currentThread().getName() + " sleep interrupted.");
                break;
            } catch (Exception e) {
                logger.log(Level.SEVERE, Thread.currentThread().getName() + " error: " + e.getMessage(), e);
            }
        }
        logger.info(Thread.currentThread().getName() + " exiting.");
    }

    public void run(int numWorkers) throws InterruptedException, IOException {
        // Setup logger to file:
        FileHandler fh = new FileHandler("rideshare.log");
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);

        // Launch workers
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i;
            Thread t = new Thread(() -> worker(workerId));
            threads.add(t);
            t.start();
        }

        // Add sample tasks
        for (int i = 1; i <= 20; i++) {
            addTask("RideRequest#" + i);
        }

        // Wait for queue to empty
        while (true) {
            Thread.sleep(200);
            queueLock.lock();
            try {
                if (taskQueue.isEmpty()) break;
            } finally {
                queueLock.unlock();
            }
        }

        // Stop workers
        for (Thread t : threads) {
            t.interrupt();
        }
        for (Thread t : threads) {
            t.join();
        }

        // Write results to file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            for (String r : results) writer.write(r + "\n");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing output.txt: " + e.getMessage(), e);
        }

        logger.info("All done.");
    }

    public static void main(String[] args) throws Exception {
        new Assignment6().run(5);
    }
}
