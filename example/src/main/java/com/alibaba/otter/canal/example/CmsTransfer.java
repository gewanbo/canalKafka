package com.alibaba.otter.canal.example;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * 单机模式的测试例子
 *
 * @author wanbo 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class CmsTransfer extends AbstractCMS {

    public CmsTransfer(String destination){
        super(destination);
    }

    public static void main(String args[]) {
        Properties config = new Properties();

        try {
            logger.info("Start to load configure file.\n");

            String configFile = System.getProperty("example.configurationFile");

            logger.info("Config file :" + configFile + SEP);

            InputStream is = new FileInputStream(configFile);
            config.loadFromXML(is);
            is.close();

            logger.info("Configure file loaded successful.\n");
        } catch (Exception e) {
            logger.error("Configure file loaded failed.", e);
            System.exit(-1);
        }

        // 根据ip，直接创建链接，无HA的功能
        String canalServer = config.getProperty("canal.server");
        int canalPort = Integer.parseInt(config.getProperty("canal.port"));
        String destination = config.getProperty("canal.instance");

        logger.info("canal: server-" + canalServer + " port-" + canalPort + " instance-" + destination + SEP);

        //AddressUtils.getHostIp()
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalServer,
                canalPort), destination, "", "");

        final CmsTransfer clientTest = new CmsTransfer(destination);
        clientTest.setConfig(config);
        clientTest.setConnector(connector);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client\n");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.\n");
                }
            }

        });
    }

}
