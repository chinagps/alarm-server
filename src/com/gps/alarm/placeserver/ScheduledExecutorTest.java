package com.gps.alarm.placeserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * http://www.w3c.com.cn/%E4%BD%BF%E7%94%A8executors-newscheduledthreadpool%E7%9A%84%E4%BB%BB%E5%8A%A1%E8%B0%83%E5%BA%A6
 * 鉴于 Timer 的上述缺陷，Java 5 推出了基于线程池设计的 ScheduledExecutor。其设计思想是，
 * 每一个被调度的任务都会由线程池中一个线程去执行，因此任务是并发执行的，相互之间不会受到干扰。
 * 需要注意的是，只有当任务的执行时间到来时，ScheduedExecutor 才会真正启动一个线程，
 * 其余时间 ScheduledExecutor 都是在轮询任务的状态。
 * 清单 2. 使用 ScheduledExecutor 进行任务调度
 * @author Administrator
 *
 * 清单 2 展示了 ScheduledExecutorService 中两种最常用的调度方法 ScheduleAtFixedRate 和 ScheduleWithFixedDelay。
 * ScheduleAtFixedRate 每次执行时间为上一次任务开始起向后推一个时间间隔，
 * 即每次执行时间为 :initialDelay, initialDelay+period, initialDelay+2*period, …；
 * ScheduleWithFixedDelay 每次执行时间为上一次任务结束起向后推一个时间间隔，
 * 即每次执行时间为：initialDelay, initialDelay+executeTime+delay, initialDelay+2*executeTime+2*delay。
 * 由此可见，ScheduleAtFixedRate 是基于固定时间间隔进行任务调度，ScheduleWithFixedDelay 取决于每次任务执行的时间长短，
 * 是基于不固定时间间隔进行任务调度。
 * 几种任务调度的 Java 实现方法与比较
 * http://www.ibm.com/developerworks/cn/java/j-lo-taskschedule/
 * 
 * 一步一步掌握线程机制(六)---Atomic变量和Thread局部变量
 * http://www.cnblogs.com/wenjiang/p/3276433.html
 */
public class ScheduledExecutorTest implements Runnable {
	private String jobName = "";

	public ScheduledExecutorTest(String jobName) {
		super();
		this.jobName = jobName;
	}

	@Override
	public void run() {
		System.out.println("execute " + jobName);
	}

	public static void main(String[] args) {
		ScheduledExecutorService service = Executors.newScheduledThreadPool(10);

		long initialDelay1 = 1;
		long period1 = 1;
		// 从现在开始1秒钟之后，每隔1秒钟执行一次job1
		service.scheduleAtFixedRate(new ScheduledExecutorTest("job1"), initialDelay1, period1, TimeUnit.SECONDS);

		long initialDelay2 = 2;
		long delay2 = 2;
		// 从现在开始2秒钟之后，每隔2秒钟执行一次job2
		service.scheduleWithFixedDelay(new ScheduledExecutorTest("job2"), initialDelay2, delay2, TimeUnit.SECONDS);
	}
}
