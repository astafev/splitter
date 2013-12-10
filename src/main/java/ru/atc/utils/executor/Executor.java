/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.atc.utils.executor;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author atc
 */
public class Executor implements Callable<Integer> {
    private final LinkedBlockingQueue<String> queue;
    private final Connection connection;
    public Integer counter;


    public Executor(Connection conn, LinkedBlockingQueue<String> queue) {
        this.connection = conn;
        this.queue = queue;
    }


    public Integer call() throws Exception {
        counter = 0;
        Statement stmt = connection.createStatement();
        while (true) {
            String s_stmt = queue.poll(5, TimeUnit.SECONDS);
            try {
                if (s_stmt == null) {
                    return counter;
                }
                s_stmt = s_stmt.trim();
                if (s_stmt.equalsIgnoreCase("commit")) {
                    System.out.println("executing batch. Counter: " + counter);
                    int[] ints = stmt.executeBatch();
                    for (int i : ints) {
                        counter += i;
                    }
                    System.out.println("executed batch. Counter: " + counter);
                    connection.commit();
                } else if (s_stmt.equalsIgnoreCase("BEGIN")) {
/*
                if(stmt!=null)
                    stmt.close();
                stmt = connection.createStatement();
*/
                } else if (s_stmt.startsWith("INSERT")) {
                    stmt.addBatch(s_stmt);
//                    stmt.executeUpdate(s_stmt);
                } else {
                    stmt.execute(s_stmt);
                }
            } catch (Exception e) {
                System.err.println("error statement:" + s_stmt);

                if(e instanceof java.sql.BatchUpdateException) {
                    ((BatchUpdateException) e).getNextException().printStackTrace();
                }
                throw e;
            }
        }
    }
}
