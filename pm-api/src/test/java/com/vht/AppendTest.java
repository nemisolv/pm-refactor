package com.vht;

public class AppendTest {
    public static void main(String[] args) throws InterruptedException {
        StringBuilder sb = new StringBuilder();

        Runnable task = () -> {
            for (int i = 0; i < 1000; i++) {
               synchronized (sb) {
                   sb.append("A");
               }
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        System.out.println("Độ dài sb = " + sb.length());
    }
}
