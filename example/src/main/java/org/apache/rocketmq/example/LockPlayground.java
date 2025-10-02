package org.apache.rocketmq.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

public class LockPlayground {
    static final Object lockA = new Object();
    static final Object lockB = new Object();
    static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : 6;
        long runMillis = args.length > 1 ? Long.parseLong(args[1]) : 60000000L;

        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            int id = i;
            Thread t = new Thread(() -> worker(id, start), "worker-" + id);
            t.start();
        }
        start.countDown();

        Thread.sleep(runMillis);
        running = false;
    }

    static void worker(int id, CountDownLatch start) {
        try { start.await(); } catch (InterruptedException ignored) {}
        ThreadLocalRandom r = ThreadLocalRandom.current();

        while (running) {
            int pick = r.nextInt(100);
            if (pick < 45) {            // 45%：争用 lockA
                contend(lockA, 3, 8);
            } else if (pick < 90) {     // 45%：争用 lockB
                contend(lockB, 5, 12);
            } else {                     // 10%：模拟业务空转/定时等待
                LockSupport.parkNanos(5_000_000); // 5ms
            }
        }
    }

    static void contend(Object lock, int preMin, int preMax) {
        busy(preMin, preMax);  // 进入锁前打散
        synchronized (lock) {
            busy(2, 6);        // 临界区忙一会
        }
    }

    static void busy(int msMin, int msMax) {
        long ms = ThreadLocalRandom.current().nextLong(msMin, msMax + 1);
        long end = System.nanoTime() + ms * 1_000_000L;
        while (System.nanoTime() < end) {
            double x = Math.sqrt(System.nanoTime() & 1023);
        }
    }
}