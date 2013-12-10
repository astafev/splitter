/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.atc.utils.executor;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author atc
 */
public class ScriptReader implements Callable<Boolean> {
    LinkedBlockingQueue<String> statements;
    boolean end = false;
    private final File file;
    public int counter = 0;

    public ScriptReader(LinkedBlockingQueue<String> statements, File file) {
        this.statements = statements;
        this.file = file;
    }

    public Boolean call() throws Exception {
        counter = 0;
        InputStream reader = new BufferedInputStream(new FileInputStream(file));
        while (!end) {
            String s_stmt;
            try{
                 s_stmt = getStatement(reader);
            } catch (Exception e) {
                throw e;
            }
            if(!statements.offer(s_stmt, 20, TimeUnit.SECONDS)) {
                break;
            }
        }
        reader.close();
        return true;
    }

    ByteBuffer bBuffer = ByteBuffer.allocate(500_000);

    String getStatement(InputStream reader) throws IOException {
        bBuffer.clear();
        byte last = 0;

        semicolon:
        try {

            boolean insideString = false;
            boolean commentString = false;
            boolean commentChunk = false;
            while (true) {
                int current = reader.read();

                switch (current) {

                    case -1: {
                        this.end = true;
                        break semicolon;
                    }
                    //-- строчный sql комментарий
                    case 45: {
                        if(current==last && !insideString) {
                            commentString = true;
                        }
                        break;
                    }
                    //перевод строки
                    case 10:
                    case 13: {
                        if(commentString) {
                            commentString = false;
                        }
                        break;
                    }
                    // '/'
                    case 47: {
                        if (last == 42 && commentChunk) {
                            commentChunk = false;
                        }

                        break;
                    }
                     // '*'
                    case 42: {
                        if (last == 47 && !insideString) {
                            commentChunk = true;
                        }
                        break;
                    }
                    // '
                    case 39: {
                        if(!commentChunk && !commentString)
                            insideString = !insideString;
                        break;
                    }
                    // ;
                    case 59: {
                        if (insideString || commentString || commentChunk) {
                            //do nothing
                            break;
                        } else break semicolon;
                    }
                }
                bBuffer.put((byte) current);
                last = (byte) current;
            }
        } catch (RuntimeException e) {
            System.out.println(new String(bBuffer.array()));
            System.out.println(bBuffer.limit() + " " + bBuffer.position());
            throw e;
        }
//        CharsetDecoder cd = Charset.forName("UTF8").newDecoder();
//        bBuffer.flip();
        counter++;
//        CharBuffer charBuffer = cd.decode(bBuffer);
        byte[] resBytes = new byte[bBuffer.position()];
        System.arraycopy(bBuffer.array(), 0, resBytes, 0, bBuffer.position());
        String result = new String(resBytes);
        String[] ss = result.split("\0");
        if (ss.length > 1) {
            throw new RuntimeException(Arrays.toString(ss));
        }
        /*int index = result.indexOf(0);
        if (index != -1) {
            result = result.substring(0, index);
            // проверять, если не нули за бортом остались
//            if(result.substring(index).)
        }
        */
        return result;
    }

}
