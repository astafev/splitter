/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.atc.utils.executor;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.*;

//import static ru.atc.utils.OldExecute.main;
//import static ru.atc.utils.OldExecute.order;


/**
 * @author atc
 */
public class Execute {

    public static final String ORDER_FILE = "order.txt";


    public static Map<String, List<File>> orderMap;

    public static void initOrder() throws URISyntaxException, IOException {
        File file = new File(ClassLoader.getSystemResource(ORDER_FILE).toURI());
        List<String> order = Files.readAllLines(file.toPath(), Charset.defaultCharset());
        orderMap = new LinkedHashMap<>(order.size());
        for (String s : order) {
            orderMap.put(s, new LinkedList<File>());
        }
    }

    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(
                System.getProperty("jdbc.conn.string","jdbc:postgresql://192.168.157.194/postgres?user=postgres&password=postgres")
        );
        connection.setAutoCommit(false);
        File directory = new File("XWIKI");
        File[] files = directory.listFiles();
//        System.out.println(Arrays.toString(files) + "\n\n\n");
//        List<File> list = Arrays.asList(files);
        LinkedHashMap<String, List<File>> map = new LinkedHashMap<String, List<File>>();
        map.putAll(orderMap);
        for (File f : files) {
            String name = getFileName(f);
            //добавляем то что не в списке
            /*if (orderMap.get(name)==null) */
            {
                List<File> list = map.get(name);
                if (list == null) {
                    list = new LinkedList<File>();
                    list.add(f);
                    map.put(name, list);
                } else list.add(f);
            }
        }

        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(500);
        Executor executor = new Executor(connection, queue);
        ExecutorService es = Executors.newFixedThreadPool(2);
        Future<Integer> executorFuture = es.submit(executor);
        System.out.println(map.toString());
//        map = new LinkedHashMap<>();
//        LinkedList<File> test = new LinkedList<>();
//        test.add(new File("XWIKI/xwikidoc66.sql"));
//        map.put("ЖОПА", test);

        Iterator<String> iter = map.keySet().iterator();
        try {
            while (iter.hasNext()) {
                String groupName = iter.next();
                System.out.println("\n\n-------------------\nexecuting " + groupName + "!!!\n--------------------");
                Iterator<File> fileIterator = map.get(groupName).iterator();

                while (fileIterator.hasNext()) {
                    File f = fileIterator.next();
                    System.out.println("\nexecuting " + f + "\n");
                    ScriptReader sr = new ScriptReader(queue, f);
                    Future<Boolean> readerFuture = es.submit(sr);
                    while (!readerFuture.isDone()) {
                        if (!executorFuture.isDone())
                            Thread.sleep(2000);
                        else {
                            es.awaitTermination(10, TimeUnit.SECONDS);
                            executorFuture.get();
                        }
                    }
                    try {
                        readerFuture.get();
                    } catch (Exception e) {
                        es.awaitTermination(10, TimeUnit.SECONDS);
                        readerFuture.get();
                    }
                    System.out.println("\n\n" + sr.counter + " strings readed");
                    System.out.println(executor.counter + " strings already writed to db\n\n");
                }
            }
        } finally {
            System.out.println("Queue:" + queue.toString());
            es.shutdown();
            System.out.println(executorFuture.get() + " writed");
            connection.close();
        }
    }


    protected static String getFileName(File f) {
        return f.getName().split("\\d*\\.")[0];
    }

    /*private static boolean searchInOrder(String groupName) {
        for (String s : order) {
            if (s.equals(order)) {
                return true;
            }
        }
        return false;
    }*/

}
