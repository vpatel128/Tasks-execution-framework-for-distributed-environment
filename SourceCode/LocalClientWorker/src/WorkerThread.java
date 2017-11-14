import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkerThread implements Runnable {

	private String task;
	private ConcurrentLinkedQueue<String> resqueue;

	public WorkerThread(String obj, ConcurrentLinkedQueue<String> resqueue) {
		// TODO Auto-generated constructor stub
		this.task= obj;
		this.resqueue=resqueue;
	}
	   
	@Override
	public void run() {
		// TODO Auto-generated method stub
		 System.out.println(Thread.currentThread().getName()+" Start. task =" +task);
	        processCommand();
	        System.out.println(Thread.currentThread().getName()+" End.");
	}
	
	 private void processCommand() {
	        try {
	        	 String str= task;
	             String[] splitStr = str.split("\\s+");
	             // perform task
	            Thread.sleep(Integer.parseInt(splitStr[1]));
	            // add task in response queue
	            resqueue.add(str);
	        } catch (InterruptedException e) {
	        	System.out.println("value="+ 1);
	            e.printStackTrace();
	        }
	    }
	 
}
