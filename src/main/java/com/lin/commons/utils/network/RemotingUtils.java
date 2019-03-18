package com.lin.commons.utils.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.*;
import java.util.Enumeration;

/**
 * @author jianglinzou
 * @date 2019/3/11 下午1:06
 */
public class RemotingUtils {

    static final Log log = LogFactory.getLog(RemotingUtils.class);


//    public static void response(final Connection conn, final ResponseCommand response) {
//        if (response == null || conn == null) {
//            return;
//        }
//        try {
//            conn.response(response);
//        }
//        catch (final NotifyRemotingException e) {
//            log.error("发送响应失败", e);
//        }
//    }

    private static String localHost = null;


    /**
     * Just for test.
     *
     * @param host
     */
    public static void setLocalHost(String host) {
        localHost = host;
    }


    public static String getLocalHost() throws Exception {
        if (localHost != null) {
            return localHost;
        }
        localHost = findLocalHost();
        return localHost;
    }


    private static String findLocalHost() throws SocketException, UnknownHostException {
        // 遍历网卡，查找一个非回路ip地址并返回
        final Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
        InetAddress ipv6Address = null;
        while (enumeration.hasMoreElements()) {
            final NetworkInterface networkInterface = enumeration.nextElement();
            final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
            while (en.hasMoreElements()) {
                final InetAddress address = en.nextElement();
                if (!address.isLoopbackAddress()) {
                    if (address instanceof Inet6Address) {
                        ipv6Address = address;
                    } else {
                        // 优先使用ipv4
                        return normalizeHostAddress(address);
                    }
                }

            }

        }
        // 没有ipv4，再使用ipv6
        if (ipv6Address != null) {
            return normalizeHostAddress(ipv6Address);
        }
        final InetAddress localHost = InetAddress.getLocalHost();
        return normalizeHostAddress(localHost);
    }


    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }



}
