import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Client {
	public static long start;
	 public static void main(String[] args) throws Exception {
	String local=args[0];
	int noofthread=Integer.parseInt(args[1]);
	String fname =  args[2]; 
	String strLine;
	// create Request queue and Response queue
	ConcurrentLinkedQueue<String> Reqqueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> Resqueue = new ConcurrentLinkedQueue<String>();
	 FileInputStream fstream = new FileInputStream(fname);
	 BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
	 // add tasks in queue
	  while ((strLine = br.readLine()) != null)   {
		  Reqqueue.add(strLine);
          }
	  ExecutorService executor = Executors.newFixedThreadPool(noofthread);
	  start = System.currentTimeMillis();
	  // call threadpool
	for (String obj = Reqqueue.poll(); obj != null; obj = Reqqueue.poll()) {
		 Runnable worker = new WorkerThread(obj,Resqueue);
		 executor.execute(worker);
	 }
	 executor.shutdown();
	 while (!executor.isTerminated()) {   }  
     System.out.println("Finished all threads");
     System.out.println("Time="+(System.currentTimeMillis() - start));
     System.out.println("value="+ 0);
	}
}
