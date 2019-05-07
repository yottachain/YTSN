package com.ytfs.service.http;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class UseSpaceHandler extends HttpHandler {

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        rspns.setContentType("text/json");
        UserCounter counter=new UserCounter();
        
        
        rspns.getWriter().write("0");
    }

    public static class UserCounter {

        private long filecount;
        private long usespace;
        private long totalcost;

        /**
         * @return the filecount
         */
        public long getFilecount() {
            return filecount;
        }

        /**
         * @param filecount the filecount to set
         */
        public void setFilecount(long filecount) {
            this.filecount = filecount;
        }

        /**
         * @return the usespace
         */
        public long getUsespace() {
            return usespace;
        }

        /**
         * @param usespace the usespace to set
         */
        public void setUsespace(long usespace) {
            this.usespace = usespace;
        }

        /**
         * @return the totalcost
         */
        public long getTotalcost() {
            return totalcost;
        }

        /**
         * @param totalcost the totalcost to set
         */
        public void setTotalcost(long totalcost) {
            this.totalcost = totalcost;
        }

    }

}
