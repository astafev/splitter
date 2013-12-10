package ru.atc.utils;

import ru.atc.utils.executor.Execute;
import ru.atc.utils.executor.Executor;
import ru.atc.utils.splitter.Splitter;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Date: 21.10.13
 * Author: astafev
 */
public class Starter {
    public static final String USAGE = "Params:\n" +
            "    -s <file>         - to split first\n" +
            "    -d <directory>    - to execute scripts in directory\n";

    public static void main(String[] args) throws Exception {
        boolean split = false;
        File scriptFile = null;
        File scriptDirectory = null;
        if(args.length == 0) {
            System.err.println(USAGE);
            return;
        }
        for(int i = 0; i<args.length; i++) {
            try {
                if(args[i].equals("-s")) {
                    scriptFile = new File(args[++i]);
                    if(!scriptFile.exists()) {
                        throw new IllegalArgumentException("File " + args[i] + " didn't found!");
                    }
                    split = true;

                } else if(args[i].equals("-d")) {
                    scriptDirectory = new File(args[++i]);
                    if(!scriptDirectory.exists()) {
                        if(split) {
                            scriptDirectory.mkdir();
                            Splitter.directory = scriptDirectory;
                        } else throw new IllegalArgumentException("Directory " + args[i] + " didn't found!");
                    } else if(split) {
                        for(File file:scriptDirectory.listFiles()) {
                            if(!file.delete()) {
                                throw new IOException("Unable to delete file " + file.getAbsolutePath());
                            }
                        }
                    }

                }
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println(e.getLocalizedMessage());
                System.err.println(USAGE);
                return;
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
                System.err.println(USAGE);
                return;
            }
        }


        if(split) {
            if(!Splitter.directory.exists()) {
                Splitter.directory.mkdir();
                Splitter.run(scriptFile);
            }
        }
        try {
            Execute.initOrder();
            Execute.main(null);
        } catch (URISyntaxException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("Probably impossible error");
        }


    }
}
