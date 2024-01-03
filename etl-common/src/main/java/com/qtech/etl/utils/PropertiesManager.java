package com.qtech.etl.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/02 11:48:03
 * desc   :  配置文件管理类
 */
public class PropertiesManager {

    // 读取Properties配置文件，单例模式
    private static PropertiesManager propertiesManager;
    private static Properties properties;
    private static String fileName;

    // 单例模式必须用private构造方法，不能用public
    private PropertiesManager() {
        properties = new Properties();
        InputStream input = PropertiesManager.class.getClassLoader().getResourceAsStream(fileName);

        try {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 提供一个入口获取PropertiesManager, 这个方可以严格控制实例生成的个数
    public static PropertiesManager getInstance() {

        // 如果为空建创建一个自己的构造方法， 获取PropertiesManager
        if (propertiesManager == null) {
            propertiesManager = new PropertiesManager();
        }
        return propertiesManager;
    }

    // 配置文件名
    public static void loadProp(String fileName) {
        PropertiesManager.fileName = fileName;
    }

    // 以上只是获取了properties的key值， 下面这个方法获取对应的value值
    public String getString(String key) { return properties.getProperty(key); }

    public Integer getInt(String key) {
        return Integer.valueOf(properties.getProperty(key));
    }

    public Double getDouble(String key) {
        return Double.valueOf(properties.getProperty(key));
    }
}
