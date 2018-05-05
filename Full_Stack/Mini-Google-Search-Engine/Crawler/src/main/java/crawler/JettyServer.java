package crawler;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class JettyServer {
    public JettyServer(){}
    public void run(String localPort) throws Exception {
        Server server = new Server(Integer.parseInt(localPort));
        WebAppContext context = new WebAppContext();
        context.setDescriptor("./src/main/webapp/WEB-INF/web.xml");
        context.setContextPath("/");
        context.setResourceBase(".");
        context.setParentLoaderPriority(false);
        server.setHandler(context);
        server.setStopAtShutdown(true);
        server.start();
        server.join();
    }
}
