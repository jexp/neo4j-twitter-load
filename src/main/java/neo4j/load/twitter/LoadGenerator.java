package neo4j.load.twitter;

import org.HdrHistogram.Histogram;
import org.neo4j.driver.v1.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * @author mh
 * @since 29.11.16
 * TODO count types of operations / histogram
 */
public class LoadGenerator {

    public static final int WARMUP = 1000;
    static boolean log = false;

    private static Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10),3);

    private static ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }

    public static void main(String[] args) throws InterruptedException {
        System.err.println("Usage: java -jar neo4j-twitter-load-1.0-SNAPSHOT-jar-with-dependencies.jar concurrency bolt+routing://host:port maxOperations-or-minus-one");
        int concurrency = args.length > 0 ? Integer.parseInt(args[0]) : Runtime.getRuntime().availableProcessors();
        String uri = args.length > 1 ? args[1] : "bolt://localhost";
        int total = args.length > 2 ? Integer.parseInt(args[2]) : -1;
        String password = "test";
        LoadGenerator loadGenerator = new LoadGenerator();
        loadGenerator.start(concurrency, uri, password, total);
        loadGenerator.stop();
    }

    private void stop() {
        histogram.outputPercentileDistribution(System.out, 1,1000.0);
    }

    private void start(int concurrency, String uri, String password, int total) throws InterruptedException {
        int maxOps = total / concurrency;
        Users users = new Users();
        AuthToken auth = AuthTokens.basic("neo4j", password);
        CountDownLatch latch = new CountDownLatch(concurrency);
        try (Driver driver = GraphDatabase.driver(uri, auth)) {
            initialize(driver,users);
            for (int i = 0; i < concurrency; i++) {
                int thread = i;
                new Thread(() -> {generateLoad(thread, driver,users,maxOps);latch.countDown();}).start();
            }
            latch.await();
        }
    }

    private void initialize(Driver driver, Users users) {
        try (Session session = driver.session(AccessMode.WRITE)) {
            initializeUsers(users, session);
            warmup(users, session);
        }
    }

    private void initializeUsers(Users users, Session session) {
        session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE;");
        users.addAll(loadUserNames(session));
        if (users.size() == 0) {
            try (Transaction tx = session.beginTransaction()) {
                for (int i = 0; i < 1000; i++) {
                    Operations.CREATE_USER.execute(tx, users.newName(), Collections.emptySet());
                }
                tx.success();
            }
        }
        System.out.println("Initialize with "+users.size()+" names.");
    }

    private void warmup(Users users, Session session) {
        long start = System.nanoTime();
        for (int i = 0; i< WARMUP; i++) {
            Operations.selectAndRun(roll(), session, session, users);
        }
        long delta = System.nanoTime() - start;
        System.out.println("Finished warmup "+WARMUP+" operations in "+ TimeUnit.NANOSECONDS.toSeconds(delta)+" seconds.");
    }

    private List<String> loadUserNames(Session session) {
        return session.run("MATCH (u:User) return u.name as name").list((r) -> r.get("name").asString());
    }


    private void generateLoad(int thread, Driver driver, Users users, int maxOps) {
        int tx = 0;
        StringBuilder sb = new StringBuilder(10000);
        long time = System.nanoTime();
        try (Session writeSession = driver.session(AccessMode.WRITE);
             Session readSession = driver.session(AccessMode.READ)) {
            while (true) {
                Operations ops = Operations.selectAndRun(roll(), writeSession, readSession, users);
                sb.append(ops.marker);
                tx++;
                if (tx % 10000 == 0) {
                    System.out.println(thread + ". TX/S: "+tx / secondsSince(time)+ " "+(log ? sb.toString():""));
                    sb.setLength(0);
                }
                if (maxOps > 0 && tx > maxOps) {
                    break;
                }
            }
        }
    }

    private static int roll() {
        return random().nextInt(100);
    }

    private long secondsSince(long time) {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - time);
    }

    public int countUsers(Session session) {
        return session.run("MATCH (:User) RETURN count(*) as c").single().get(0).asInt();
    }

    interface Action {
        void execute(Transaction tx, String name, Object value);
    }
    enum Operations implements Action {
        CREATE_USER(1,true) {
            public void execute(Transaction tx, String name, Object value) {
                tx.run(
                        "CREATE (u:User {name:{name},created:timestamp()}) " +
                                "WITH u " +
                                "UNWIND {friends} as friend " +
                                "MATCH (f:User {name:friend}) " +
                                "CREATE (u)-[:FOLLOWS {time:timestamp()}]->(f)",
                        parameters("name", name, "friends", value)).consume();
            }
        }, TWEET(15,true) {
            public void execute(Transaction tx, String name, Object value) {
                tx.run("MATCH (u:User {name:{name}})\n" +
                                "CREATE (u)-[:TWEETS]->(t:Tweet {text:{text},time:timestamp()})\n" +
                                "WITH (u),(t) " +
                                "MATCH (u)<-[:FOLLOWS]-(f)\n" +
                                "CREATE (f)-[:STREAM]->(t);",
                        parameters("name", name, "text", "A Tweet by " + name + " at " + new Date())).consume();
            }
        }, READ_TWEETS(76,false) {
            public void execute(Transaction tx, String name, Object value) {
                int count = 0;
                StatementResult result = tx.run("MATCH (u:User {name:{name}})-[:STREAM]->(t:Tweet)\n" +
                        "RETURN t.text, t.time\n" +
                        "ORDER BY t.time DESC LIMIT 10;", parameters("name", name));
                while (result.hasNext()) {
                    result.next();
                    count++;
                }
            }
        }, FOLLOW_RECOMMENDATION(8,true) {
            public void execute(Transaction tx, String name, Object value) {
                StatementResult result = tx.run("MATCH (u:User {name:{name}})-[:FOLLOWS]->(f)\n" +
                                "WITH u,f ORDER BY rand() LIMIT 20\n" +
                                "MATCH (f)-[r:FOLLOWS]->(f2) // todo move up for triadic closure\n" +
                                "WHERE NOT (u)-[:FOLLOWS]->(f2)\n" +
                                "// todo that's also the natural graph relationship retrieval order, latest followers first\n" +
                                "WITH f, f2 ORDER BY r.time DESC \n" +
                                "WITH f, collect(f2)[..30] as top30\n" +
                                "UNWIND top30 as reco\n" +
                                "WITH reco, count(*) as freq\n" +
                                "ORDER BY freq DESC LIMIT 5\n" +
                                "RETURN reco.name as name;\n",
                        parameters("name", name));
                List<String> friends = result.list((r) -> r.get("name").asString());
                if ((roll() - 30) < 0 && friends.size() > 0) {
                    String friend = friends.get(random().nextInt(friends.size()));
                    tx.run("MATCH (u:User {name:{name}}),(f:User {name:{friend}})" +
                                    " CREATE (u)-[:FOLLOWS {time:timestamp()}]->(f)",
                    parameters("name", name, "friend", friend)).consume();
                }
            }
        };


        public final int chance;
        public final boolean writes;
        public final char marker = name().charAt(0);
        Operations(int chance, boolean writes) { this.chance = chance; this.writes = writes; }

        public static Operations selectAndRun(int chance, Session writeSession, Session readSession, Users users) {
            if ((chance -= CREATE_USER.chance) < 0) {
                return run(CREATE_USER,users.newName(),users.randomUsers(10), writeSession,users);
            } else {
                Operations ops = null;
                if ((chance -= TWEET.chance) < 0) ops = TWEET;
                else if ((chance -= READ_TWEETS.chance) < 0) ops = READ_TWEETS;
                else if ((chance -= FOLLOW_RECOMMENDATION.chance) < 0) ops = FOLLOW_RECOMMENDATION;
                return ops == null ? null : run(ops, null, null, ops.writes ? writeSession : readSession, users);
            }
        }

        private static Operations run(Operations ops, String user, Object param, Session session, Users users) {
            String bookmark = null;
            Transaction tx;
            if (user != null) {
                tx = session.beginTransaction();
            } else {
                user = users.randomUser();
                bookmark = users.bookmarkFor(user);
                tx = session.beginTransaction(bookmark);
            }
            long start = System.nanoTime();
            try {
                ops.execute(tx, user, param);
                tx.success();
            } catch(Exception e) {
                System.err.println(e.getMessage());
            } finally {
                tx.close();
            }
            long delta = System.nanoTime() - start; // TODO HDR
            histogram.recordValue(delta);
            String newBookmark = session.lastBookmark();
            if (newBookmark!= null && !newBookmark.equals(bookmark)) {
                users.bookmark(user, newBookmark);
            }
            return ops;
        }
    }
}
