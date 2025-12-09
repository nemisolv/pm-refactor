package com.vht;

public class DemoWithoutSync {
    private static final StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws InterruptedException {
        Runnable task = () -> {
            for (int i = 0; i < 5; i++) {
                sb.append(Thread.currentThread().getName())
                  .append(" -> i=")
                  .append(i)
                  .append("\n");
            }
        };

        // Tạo 3 thread chạy song song
        Thread t1 = new Thread(task, "T1");
        Thread t2 = new Thread(task, "T2");
        Thread t3 = new Thread(task, "T3");

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        System.out.println("Kết quả (KHÔNG synchronized):\n" + sb);
    }
}
