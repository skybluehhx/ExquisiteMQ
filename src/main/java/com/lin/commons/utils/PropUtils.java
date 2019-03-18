package com.lin.commons.utils;

import java.io.*;
import java.util.Properties;

/**
 * @author jianglinzou
 * @date 2019/3/8 下午6:44
 */
public class PropUtils {

    public static Properties getResourceAsProperties(String resource, String encoding) throws IOException {
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream(resource);
        }
        catch (IOException e) {
            File file = new File(resource);
            if (!file.exists()) {
                throw e;
            }
            in = new FileInputStream(file);
        }

//        InputStreamReader reader = new InputStreamReader(in, encoding);
        Properties props = new Properties();
        props.load(in);
        in.close();
//        reader.close();

        return props;

    }


    public static File getResourceAsFile(String resource) throws IOException {
        try {
            return new File(ResourceUtils.getResourceURL(resource).getFile());
        }
        catch (IOException e) {
            return new File(resource);
        }
    }

    public abstract static class Action {
        public abstract void process(String line);


        public boolean isBreak() {
            return false;
        }
    }


    public static void processEachLine(String string, Action action) throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(string));
        try {
            String line;
            while ((line = br.readLine()) != null) {
                action.process(line);
                if (action.isBreak()) {
                    break;
                }
            }

        }
        finally {
            br.close();
            br = null;
        }
    }

}
