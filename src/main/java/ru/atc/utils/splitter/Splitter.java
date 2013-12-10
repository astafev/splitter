package ru.atc.utils.splitter;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Scanner;

/**
 * Date: 03.10.13
 * Author: atc
 */
public class Splitter {
    //"/home/atc/svn/Имущество/NEW_SYSTEM/DB/scripts/XWIKI/XWIKI_INS.sql"
    public static final String USAGE = "put filename as parameter";

    public static void main(String[] args) throws IOException {
        if(args.length==0) {
            System.err.println(USAGE);
            return;
        }
        File file = new File(args[0]);
        if(!file.exists()) {
            System.err.println("File " + args[0] + " wasn't been found.");
            System.err.println(USAGE);
            return;
        }
        run(file);

    }

    public static void run(File file) throws IOException {
        InputStreamReader source = new InputStreamReader(
                new BufferedInputStream(new FileInputStream(file)),
                Charset.forName(System.getProperty("script_charset", "windows-1251"))
        );
        Scanner scanner = new Scanner(source);

        StringBuilder sb = new StringBuilder();
        String tableName = getTableName(scanner, sb);
        OutputStream out = createFile(tableName);
        out.write(sb.toString().getBytes());
        int counter = 0;
        while (scanner.hasNext()) {

            String line = scanner.nextLine();
            counter++;
            if (line.startsWith("SET")) {
                if (out != null)
                    out.close();
                sb = new StringBuilder("\n");
                tableName = getTableName(scanner, sb);
                out = createFile(tableName);
                out.write(line.getBytes());
                out.write(sb.toString().getBytes());
            } else {
                out.write(line.getBytes());
            }

            out.write(0x000A);
        }
        System.out.println(counter + " lines readed");
        if (out != null)
            out.close();
        scanner.close();
    }

    static public File directory = new File(System.getProperty("splitter.directory", "XWIKI/"));
    static OutputStream createFile(String tableName) throws FileNotFoundException {
        File file = new File(directory, tableName + ".sql");
        int counter = 0;
        while (file.exists()) {
            file = new File(directory, tableName + counter + ".sql");
            counter++;
        }
        System.out.println("creating " + file.getAbsolutePath());
        return new BufferedOutputStream(new FileOutputStream(file));
    }

    static String getTableName(Scanner scanner, StringBuilder sb) {

        while (true) {
            String line = scanner.nextLine();
            sb.append(line).append('\n');
            if (line.startsWith("INSERT")) {
                return line.split(" ")[2];
            }
        }
    }
}
