import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost");

        // ping
        System.out.println("Pinging server: " + jedis.ping());

        // basic get and set
        jedis.set("Say My Name", "DL");
        System.out.println(jedis.get("Say My Name"));

        // list  operation
        jedis.lpush("list", "Redis");
        jedis.lpush("list", "Mongodb");
        jedis.lpush("list", "MySQL");

        // it is fine if the end of the range is out of bound
        List<String> list = jedis.lrange("list", 0, 8);

        System.out.println("Content of 'list': "+list);

        // get all the saved keys back, as a set
        Set<String> keySet = jedis.keys("*");
        System.out.println("keys in redis: " + keySet);


        // try out set operation
        jedis.sadd("set", "a");
        jedis.sadd("set", "a");
        jedis.sadd("set", "b");
        jedis.sadd("set", "c");

        Set<String> set = jedis.smembers("set");
        // set may not have order
        System.out.println("Content of a set: "+set);
        System.out.println("does c exist?: "+jedis.sismember("set", "c"));


        // hashmap!
        jedis.hset("user1", "name", "Peter");
        jedis.hset("user1", "job", "Spiderman");

        System.out.println("my job is: "+jedis.hget("user1", "job"));
        System.out.println("the whole map: " + jedis.hgetAll("user1"));

        // sorted set
        // each member has a associated ranking. for sorting purposes
        Map<String, Double> scores = new HashMap<String, Double>();
        scores.put("PlayerOne", 3000.0);
        scores.put("PlayerThree", 8200.0);
        scores.put("PlayerTwo", 1500.0);

        for(Map.Entry<String, Double> entry: scores.entrySet()) {
            jedis.zadd("ranking", entry.getValue(), entry.getKey());
        }

        System.out.println("Print everything out: " + jedis.zrange("ranking", 0, 100));
        System.out.println("The rank of player 3 is: " + jedis.zrevrank("ranking", "PlayerThree"));
        System.out.println("The rank of player 1 is: " + jedis.zrevrank("ranking", "PlayerOne"));

        //Transaction
        /*
            Allow users to execute group of commands in 1 single step

            all commands in a transaction are sequentially executed as a single isolated operation
            can't issue a request by another client in the middle of the execution of redis transaction

            atomic

            MULTI
            ...
            ...
            EXEC


            After MULTI, redis will queue all the commands, and EXEC will have them run in 1 step

            WATCH works by making EXEC conditional

            Redis only perform a transaction if the WATCH keys were not modified
            if modified, the transaction won't be entered

            WATCH monitor the changes of the key between the WATCH is issued until EXEC is called.
            after that, all keys UNWATCHed

            example:

            WATCH sampleKey
            num = GET sampleKey
            num = num + 1
            MULTI
            SET sampleKey $num
            EXEC
        */
        String friendsPrefix = "friends#";
        String userOneId = "315602";
        String userTwoId = "574241";

        Transaction transaction = jedis.multi();
        transaction.sadd(friendsPrefix + userOneId, userTwoId);
        transaction.sadd(friendsPrefix+userTwoId, userOneId);
        transaction.exec();

        System.out.println(jedis.smembers(friendsPrefix + userOneId));
        System.out.println(jedis.smembers(friendsPrefix+userTwoId));


        // pipeline
        /*
            use to send multiple commands in one request to save connection time
            as long as the operations are mutually independent
        */

        // pub & sub
        Thread thread = new Thread(new Subscriber());
        thread.start();

        Jedis publisher = new Jedis();
        publisher.publish("channel", "This is a test message");
    }


}

/*
    Use a separate client:

    redis-cli

    # check available channels
    PUBSUB CHANNELS

    # publish message through channel
    PUBLISH channel 'hello hlelo'
*/
class Subscriber implements Runnable {
    public void run() {
        Jedis subscriber = new Jedis();
        subscriber.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("Got a message: " + message);
            }
        }, "channel");
    }
}
