//package com.guyi.study;
//
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.FutureTask;
//
///**
// * 课题 : 公司项目中Java的多线程一般用在哪些场景？
// * 目的 : 1. 吞吐量； web，容器帮我们做了多线程，但是只能做请求的层面
// *        2. 伸缩性； 增加cpu核数提高性能；若是单线程，执行到单核。
// * 功能 : 多线程联系
// * date : 2021-03-06 13:06
// * package : com.guyi.study
// *
// * @author : 谷燚
// * @version : 1.0
// */
//public class Multithreading {
//
//}
////public class MyServlet extends HttpServlet{
////    private static Map<String, String> fileNameData = new HashMap<String, String>();
////    private void processFile3(String fileName){
////        String data = fileNameData.get(fileName);
////        if (data == null){
////            data = readFromFile(fileName); // 耗时28ms
////            fileNameData.put(fileName,data);
////        }
////        // process with data
////    }
////}
////public class MyServlet extends HttpServlet{
////    private static ConcurrentHashMap<String, String> fileNameData = new ConcurrentHashMap<String, String>();
////    private void processFile3(String fileName){
////        String data = fileNameData.get(fileName);
////        if (data == null){
////            data = readFromFile(fileName); // 耗时28ms
////            fileNameData.put(fileName,data);
////        }
////        // process with data
////    }
////}
//public class MyServlet extends HttpServlet{
//    private static ConcurrentHashMap<String, String> fileNameData = new ConcurrentHashMap<String, String>();
//    private void processFile3(String fileName){
//        String data = fileNameData.get(fileName);
//        //  “偶然”-- 1000个线程同时到这里，同时发现data为null
//        if (data == null){
//            data = readFromFile(fileName); // 耗时28ms
//            FutureTask old = fileNameData.putIfAbsent(fileName,data);
//            if (old == null){
//                data = old;
//            }else{
//                exec.execute(data);
//            }
//        }
//        String d = data.get();
//        // process with data
//    }
//    private FutureTask newFutureTask(final String file){
//        return newFutureTask(new Callable<String>(){
//            public String call(){
//                return readFromFile(file);
//            }
//        private String readFromFile(String file){ return "";}
//    }
//}
//}
