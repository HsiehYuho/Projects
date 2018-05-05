package edu.upenn.cis455.mapreduce.master;

import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class MinimalJettyServer  {

    static int numOfWorkers = 1;
    static String job = "edu.upenn.cis455.mapreduce.job.IndexJob";
    static String inputDir = "/input";
    static String outputDir = "/output";
    static String numOfMapThreads = "10";
    static String numOfReduceThreads = "10";

    /** Get parameters from command line. */
    private static void getParameters(String[] args) {
        if (args.length == 0) {
            System.out.println("*** Author: Lanqing Yang (lanqingy)");
            System.exit(1);
        } else {
            try {
                if (args.length >= 1) numOfWorkers = Integer.parseInt(args[0]);
                if (args.length >= 2) job = args[1];
                if (args.length >= 3) inputDir = args[2];
                if (args.length >= 4) outputDir = args[3];
                if (args.length >= 5) numOfMapThreads = args[4];
                if (args.length >= 6) numOfReduceThreads = args[5];
            } catch (Exception e) {
                System.err.println("Please specify:\n" +
                        "1) MapReduce Job classpath\n" +
                        "2) Input directory that worker should read from\n" +
                        "3) Output directory that worker should write to\n" +
                        "4) Number of map executors (default: 4)\n" +
                        "5) Number of reduce executors (default: 4)\n");
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        getParameters(args);

        Server server = new Server(8080);

        WebAppContext context = new WebAppContext();

        context.setDescriptor("conf/web.xml");
        context.setContextPath("/");
        context.setResourceBase(".");
        context.setParentLoaderPriority(false);
        server.setHandler(context);

        server.setStopAtShutdown(true); // destroy all servlets when the JVM is shutdown after SIGINT from Ctrl+C

        server.start();

        while (!MasterServlet.workersReady(numOfWorkers)) {
            Thread.sleep(100);
        }
        MasterServlet.createMapReduce(job, inputDir, outputDir, numOfMapThreads, numOfReduceThreads);

        server.join();
    }
}
