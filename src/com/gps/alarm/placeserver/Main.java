package com.gps.alarm.placeserver;

import java.awt.geom.Point2D;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class Main {

	private static Logger logger = Logger.getLogger(Main.class);

	public static void main(String[] args) throws FileNotFoundException, IOException {
		// 读取log4j配置
		PropertyConfigurator.configure("conf/log4j.properties");
		// 读取数据库配置
		final Properties properties = new Properties();
		properties.load(new FileInputStream("conf/db.properties"));

		// 设置数据库
		DataBase.setDataSource(
			properties.getProperty("db_driver"), 
			properties.getProperty("db_url"), 
			properties.getProperty("db_username"), 
			properties.getProperty("db_password")
		);

		// 获取初始参数
		final long startTime = Long.parseLong(properties.getProperty("start_time"));
		final long endTime = Long.parseLong(properties.getProperty("end_time"));
		// 线程数
		final int threadCount = Integer.parseInt(properties.getProperty("thread_count", "1"));

		logger.info(String.format("startTime: (%s), endTime: (%s),", startTime, endTime));
		
		final ScheduledExecutorService es = Executors.newScheduledThreadPool(threadCount);
		
		final long step = 300*1000;// 每次递进5分钟

		final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		
		ses.scheduleWithFixedDelay(new Runnable() {
			private final int FrameCount = 1;
			long this_time = Long.parseLong(properties.getProperty("this_time"));

			AtomicInteger gate = new AtomicInteger(-1);
			public void run() {
				try {
					if (gate.get() > 0) {
						return;
					}
					if (gate.get() == 0) {
						logger.info(String.format(threadCount + " complete: %s update place success", this_time));

						properties.setProperty("this_time", this_time + "");
						properties.store(new FileOutputStream("conf/db.properties"), null);
					}
					gate.set(threadCount);

					logger.info(String.format(threadCount + " corePoolSize start"));

					for (int i = 0; i < threadCount; i++) {
						es.schedule(makeTask(this_time, step), 0, TimeUnit.MILLISECONDS);

						this_time += step * FrameCount;
						if (this_time >= endTime) {
							logger.info("Good! all alarm no place updated!");
							ses.shutdown();
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			private Runnable makeTask(final long startTime, final long interval) {
				return new Runnable() {
					public void run() {
						try {
							// 准备好没有位置的报警
							Alarm[] alarms = queryAlarmsNoPlace(startTime, startTime+interval);
							logger.info(String.format("%s - %s, fetch %s counts alarms success ", startTime, startTime + step * FrameCount, alarms.length));
							int i = gate.decrementAndGet(); // success									
							if (alarms.length >0) {
								try {
									store(alarms);// 入库
									logger.info(String.format("%s - %s, store success. %s remaining.", startTime, interval, i));
								} catch (Exception e) {
									logger.error(String.format("%s - %s, store failed", startTime, interval));
									es.schedule(this, 100, TimeUnit.MILLISECONDS); // 重试本次任务
								}
							}
						} catch (Exception e) {
							logger.error("unknown exception", e);
							es.schedule(this, 10, TimeUnit.SECONDS); // 重试本次任务
						}
					}
					
				};
			}
		}, 1, 1, TimeUnit.MILLISECONDS);
	}

	private static Alarm[] queryAlarmsNoPlace(long startTime, long endTime) {
		logger.info(String.format("Starting get %s to %s alarms...", startTime, endTime));
		ConcurrentHashMap<Long, Alarm> alarms = new ConcurrentHashMap<Long, Alarm>();
		String sql = "select id, pos_x, pos_y, alarm_place from alarm_log where alarm_place is null and alarm_time > " + startTime +" and alarm_time <= " + endTime;
		
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;

		logger.info(String.format("Starting get alarms location desc", startTime, endTime));
		long t0 = System.currentTimeMillis();
		try {
			conn = DataBase.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();

			HashMap<Long, Long> currAlarmCount = new HashMap<Long, Long>();
			ConcurrentHashMap<Long, Alarm> currAlarms = new ConcurrentHashMap<Long, Alarm>();
			while (rs.next()) {
				Alarm alarm = new Alarm();
				alarm.setId(rs.getLong("ID"));
				alarm.setX(rs.getDouble("pos_x"));
				alarm.setY(rs.getDouble("pos_y"));
				if(rs.getDouble("pos_x") == 0.0){
					alarm.setPlace("定位无效");
				}else{
					alarm.setPlace(getAddressByCttic(new Point2D.Double(rs.getDouble("pos_x"), rs.getDouble("pos_y"))));
				}
				currAlarms.put(alarm.getId(), alarm);
				System.out.print(".");
			}
			logger.info(String.format("Complete get location desc ", startTime, endTime));
			alarms = currAlarms;	// swap
		} catch (Exception e) {
			logger.error("查询错误", e);
		} finally {
			DataBase.close(rs);
			DataBase.close(ps);
			DataBase.close(conn);
		}
		
		Collection<Alarm> a = alarms.values();
		logger.info("Get " + a.size() + " location desc in " + (System.currentTimeMillis() - t0) + " Milliseconds.");
		return (Alarm[]) a.toArray(new Alarm[a.size()]);
	}
	
	private static void store(Alarm[] alarms) throws Exception {
		logger.info("update alarm place start");
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < alarms.length; i++) {
			updateAlarmPlace(alarms[i]);
		}
		logger.info(System.currentTimeMillis() + " update alarm  " + alarms.length + " places in " + (System.currentTimeMillis() - t0) + " Milliseconds.");
		logger.info("update alarm place finish");
	}
	
	public static void updateAlarmPlace(Alarm alarm) {
		Connection conn = DataBase.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("update alarm_log set alarm_place = ? where id=?");
			ps.setString(1, alarm.getPlace());
			ps.setLong(2, alarm.getId());
			ps.execute();
		} catch (Exception e) {
			logger.error(String.format("update alarm place faild %s %s", alarm.getId(), alarm.getPlace()), e);
		} finally {
			DataBase.close(ps);
			DataBase.close(conn);
		}
	}
	
	public static String getAddressByCttic(Point2D pt){
		String result = "";
		HttpClient client = new HttpClient();
		client.getHostConfiguration().setHost("e.gis.cttic.cn", 9000);
		String url = "/SE_RGC2?st=Rgc2&point=" + pt.getX() + "," + pt.getY() + "&type=11&uid=kunlian";
		HttpMethod method = new GetMethod(url);
		
		InputStream strm = null;
		try {
			if (client.executeMethod(method) == 200) {
				SAXBuilder sab = new SAXBuilder();
				Document doc = null;
				try {
					strm = method.getResponseBodyAsStream();
					doc = sab.build(strm);
				} catch (Exception ex) {
					logger.error(method.getResponseBodyAsString());
				}
				Element root = doc.getRootElement();
				String st = root.getChild("status").getText();
				if (!st.equals("error")) {
					Element road = root.getChild("result").getChild("road");
					result += root.getChild("result").getChildText("district_text")+";";
					result += root.getChild("result").getChildText("road_address")+";";
					result += root.getChild("result").getChildText("address")+"附近";
					result += " ("+road.getChildText("road_level")+" 限速"+road.getChildText("limit_speed")+"km/h)";
				}
			}
		} catch (Exception ex) {
			logger.error("location desc failed for " + pt, ex);
			return String.format("(获取位置描述失败，经纬度: %s,%s)", pt.getX(), pt.getY());
		} finally {
			if (strm != null) {
				try {
					strm.close();
				} catch (IOException e) {
				}
			}
			method.releaseConnection();
		}
		logger.debug(String.format("%s:(%s,%s)=%s", System.currentTimeMillis(), pt.getX(), pt.getY(), result));
		return result;
	}

}
