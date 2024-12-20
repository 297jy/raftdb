package org.zhuanyi.common;

import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LogUtils {

    private static final Logger LOGGER = Logger.getLogger(LogUtils.class.toString());

    static {
        try {
            // 创建 FileHandler 对象，并指定输出文件的路径和文件名
            Handler fileHandler = new FileHandler("/log/jfraftdb.log");
            // 设置输出格式
            fileHandler.setFormatter(new SimpleFormatter());
            // 将 FileHandler 添加到 Logger 中
            LOGGER.addHandler(fileHandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void info(String msg) {
        LOGGER.info(msg);
    }
}
