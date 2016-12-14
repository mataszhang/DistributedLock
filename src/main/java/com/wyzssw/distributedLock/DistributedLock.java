
package com.wyzssw.distributedLock;


import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

/**
 * redis实现的distributedlock ,锁占用时间不宜过长
 * 
 * @see http://www.jeffkit.info/2011/07/1000/
 * @author wyzssw
 */
public class DistributedLock {
	private final String host;

	private final Integer port;
	// 锁过期时间，单位毫秒
	private final long lockTimeOut;

	private long perSleep;

	public DistributedLock(String host, Integer port, long lockTimeOut) {
		super();
		this.host = host;
		this.port = port;
		this.lockTimeOut = lockTimeOut;
		this.perSleep = 10;
	}

	public DistributedLock(String host, Integer port, long lockTimeOut, long perSleep) {
		super();
		this.host = host;
		this.port = port;
		this.lockTimeOut = lockTimeOut;
		this.perSleep = perSleep;
	}

	/**
	 * 得不到锁立即返回，得到锁返回设置的超时时间
	 * 
	 * @param key
	 * @return
	 */
	public long tryLock(String key) {
		// 得到锁后设置的过期时间，未得到锁返回0
		long expireTime = 0;
		Jedis jedis = null;
		jedis = new Jedis(host, port);
		expireTime = System.currentTimeMillis() + lockTimeOut + 1;
		if (jedis.setnx(key, String.valueOf(expireTime)) == 1) {
			// 得到了锁返回
			return expireTime;
		} else {
			String curLockTimeStr = jedis.get(key);
			// 判断是否过期
			if (StringUtils.isBlank(curLockTimeStr) || System.currentTimeMillis() > Long.valueOf(curLockTimeStr)) {
				expireTime = System.currentTimeMillis() + lockTimeOut + 1;
				curLockTimeStr = jedis.getSet(key, String.valueOf(expireTime));
				// 仍然过期,则得到锁
				if (StringUtils.isBlank(curLockTimeStr) || System.currentTimeMillis() > Long.valueOf(curLockTimeStr)) {
					return expireTime;
				} else {
					return 0;
				}
			} else {
				return 0;
			}
		}
	}

	/**
	 * 得到锁返回设置的超时时间，得不到锁等待
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 */
	public long lock(String key) throws InterruptedException {
		long sleep = (perSleep == 0 ? lockTimeOut / 10 : perSleep);
		// 得到锁后设置的过期时间，未得到锁返回0
		long expireTime = 0;
		Jedis jedis = new Jedis(host, port);
		for (;;) {
			expireTime = System.currentTimeMillis() + lockTimeOut + 1;
			if (jedis.setnx(key, String.valueOf(expireTime)) == 1) {
				// 得到了锁返回
				return expireTime;
			} else {
				String curLockTimeStr = jedis.get(key);
				// 判断是否过期
				if (StringUtils.isBlank(curLockTimeStr) || System.currentTimeMillis() > Long.valueOf(curLockTimeStr)) {
					expireTime = System.currentTimeMillis() + lockTimeOut + 1;

					curLockTimeStr = jedis.getSet(key, String.valueOf(expireTime));
					// 仍然过期,则得到锁
					if (StringUtils.isBlank(curLockTimeStr) || System.currentTimeMillis() > Long.valueOf(curLockTimeStr)) {
						return expireTime;
					} else {
						Thread.sleep(sleep);
					}
				} else {
					Thread.sleep(sleep);
				}
			}
		}
	}

	/**
	 * 先判断自己运行时间是否超过了锁设置时间，是则不用解锁
	 * 
	 * @param key
	 * @param expireTime
	 */
	public void unlock(String key, long expireTime) {
		if (System.currentTimeMillis() - expireTime > 0) {
			return;
		}
		Jedis jedis = new Jedis(host, port);
		String curLockTimeStr = jedis.get(key);
		if (StringUtils.isNotBlank(curLockTimeStr) && Long.valueOf(curLockTimeStr) > System.currentTimeMillis()) {
			jedis.del(key);
		}
	}

}
