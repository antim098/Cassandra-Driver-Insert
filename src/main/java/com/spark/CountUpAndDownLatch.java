package com.spark;

import java.util.concurrent.CountDownLatch;

/**
 * The Class CountUpAndDownLatch.
 */
public class CountUpAndDownLatch {

    /** The latch. */
    private CountDownLatch latch;

    /** The lock. */
    private final Object lock = new Object();

    /**
     * Instantiates a new count up and down latch.
     *
     * @param count the count
     */
    public CountUpAndDownLatch(int count) {
        this.latch = new CountDownLatch(count);
    }

    /**
     * Count down or wait if zero.
     *
     * @throws InterruptedException the interrupted exception
     */
    public void countDownOrWaitIfZero() throws InterruptedException {
        synchronized (lock) {
            while (latch.getCount() == 0) {
                lock.wait();
            }
            latch.countDown();
            lock.notifyAll();
        }
    }

    /**
     * Count down.
     */
    public void countDown() {
        synchronized (lock) {
            latch.countDown();
            lock.notifyAll();
        }
    }

    /**
     * Wait until zero.
     *
     * @throws InterruptedException the interrupted exception
     */
    public void waitUntilZero() throws InterruptedException {
        synchronized (lock) {
            while (latch.getCount() != 0) {
                lock.wait();
            }
        }
    }

    /**
     * Count up.
     */
    public void countUp() { // should probably check for Integer.MAX_VALUE
        synchronized (lock) {
            latch = new CountDownLatch((int) latch.getCount() + 1);
            lock.notifyAll();
        }
    }

    /**
     * Gets the count.
     *
     * @return the count
     */
    public int getCount() {
        synchronized (lock) {
            return (int) latch.getCount();
        }
    }
}
