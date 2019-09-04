package com.ytfs.service;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BpList {

    private static final List<String> bplist = new ArrayList();
    private static String localIp;

    public static void init() {
        try {
            URL url = new URL(ServerConfig.eosURI);
            localIp = url.getHost();
            SuperNode[] snlist = SuperNodeList.getSuperNodeList();
            for (SuperNode sn : snlist) {
                parseUrl(sn.getAddrs());
            }
        } catch (MalformedURLException ex) {
            bplist.add(ServerConfig.eosURI);
        }
    }

    private static void parseUrl(List<String> addrs) {
        for (String addr : addrs) {
            if (addr.toLowerCase().startsWith("/ip4/")) {
                addr = addr.substring(5);
                int index = addr.indexOf("/");
                if (index > 0) {
                    addr = addr.substring(0, index);
                    if (!localIp.equalsIgnoreCase(addr)) {
                        String url = ServerConfig.eosURI.replace(localIp, addr);
                        bplist.add(url);
                        break;
                    }
                }
            }
        }
    }
}
