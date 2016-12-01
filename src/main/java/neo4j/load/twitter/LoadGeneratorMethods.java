package neo4j.load.twitter;

import org.HdrHistogram.Histogram;
import org.neo4j.driver.v1.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * @author mh
 * @since 29.11.16
 * TODO count types of operations / histogram
 */
public class LoadGeneratorMethods {

    static boolean log = false;

    private static ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }

    public static void main(String[] args) throws InterruptedException {
        int concurrency = Runtime.getRuntime().availableProcessors();
        LoadGeneratorMethods loadGenerator = new LoadGeneratorMethods();
        loadGenerator.start(concurrency, "bolt://localhost", "test");
    }

    private void start(int concurrency, String uri, String password) throws InterruptedException {
        Users users = new Users();
        AuthToken auth = AuthTokens.basic("neo4j", password);
        try (Driver driver = GraphDatabase.driver(uri, auth)) {
            initialize(driver,users);
            for (int i = 0; i < concurrency; i++) {
                int thread = i;
                new Thread(() -> generateLoad(thread, driver,users)).start();
            }
            Thread.currentThread().join();
        }
    }

    private void initialize(Driver driver, Users users) {
        try (Session session = driver.session()) {
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE;");
            users.addAll(loadUserNames(session));
            if (users.size() == 0) {
                for (int i = 0; i < 1000; i++) createUser(session, users.newName(), Collections.emptySet());
            }
            System.out.println("Initialize with "+users.size()+" names.");
        }
    }

    private List<String> loadUserNames(Session session) {
        return session.run("MATCH (u:User) return u.name as name").list((r) -> r.get("name").asString());
    }


    private void generateLoad(int thread, Driver driver, Users users) {
        int tx = 0;
        long time = System.nanoTime();
        try (Session session = driver.session()) {
            while (true) {
                executeOperation(roll(), session, users);
                tx++;
                if (tx % 10000 == 0) {
                    System.out.println(thread + ". TX/S: "+tx / secondsSince(time));
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

    private void createUser(Session session, String name, Collection<String> friends) {
        if (log) System.out.println("Create User "+name);
        session.run(
                "CREATE (u:User {name:{name},created:timestamp()}) " +
                        "WITH u " +
                        "UNWIND {friends} as friend " +
                        "MATCH (f:User {name:friend}) " +
                        "CREATE (u)-[:FOLLOWS {time:timestamp()}]->(f)",
                parameters("name", name, "friends", friends)).consume();
    }

    private void executeOperation(int chance, Session session, Users users) {
        if ((chance -= 1) < 0)
            createUser(session, users.newName(), users.randomUsers(10));
        else if ((chance -= 15) < 0)
            createTweet(session, users.randomUser());
        else if ((chance -= 76) < 0)
            readStream(session, users.randomUser());
        else if ((chance -= 8) < 0)
            followRecommendation(session, users.randomUser());
    }

    private void followRecommendation2(Session session, String name) {
        if (log) System.out.println("Follow recommendation for "+name);
        StatementResult result = session.run("MATCH (u:User {name:{name}})-[:FOLLOWS]->(f)\n" +
                        "WITH u,f ORDER BY rand() LIMIT 20\n" +
                        "MATCH (f)-[r:FOLLOWS]->(f2) // todo move up for triadic closure\n" +
                        "WHERE NOT (u)-[:FOLLOWS]->(f2)\n" +
                        "// todo that's also the natural graph relationship retrieval order, latest followers first\n" +
                        "WITH f, f2 ORDER BY r.time DESC \n" +
                        "WITH f, collect(f2)[..30] as top30\n" +
                        "UNWIND top30 as reco\n" +
                        "RETURN reco.name as name, count(*) as freq\n" +
                        "ORDER BY freq DESC LIMIT 5;\n",
                parameters("name", name));
        List<String> friends = result.list((r) -> r.get("name").asString());
        ThreadLocalRandom random = random();
        int chance = random.nextInt(100);
        if ((chance - 30) < 0 && friends.size() > 0) {
            String friend = friends.get(random.nextInt(friends.size()));
            followUser(session, name, friend);
        }
    }
    private void followRecommendation(Session session, String name) {
        if (log) System.out.println("Follow recommendation for "+name);
        StatementResult result = session.run("MATCH (u:User {name:{name}})-[:FOLLOWS]->(f)-[r:FOLLOWS]->(f2)\n" +
                        "WHERE NOT (u)-[:FOLLOWS]->(f2)\n" +
                        "WITH f, f2 ORDER BY r.time DESC \n" +
                        "WITH f, collect(f2)[..30] as top30\n" +
                        "UNWIND top30 as reco\n" +
                        "RETURN reco.name as name, count(*) as freq\n" +
                        "ORDER BY freq DESC LIMIT 5;\n",
                parameters("name", name));
        List<String> friends = result.list((r) -> r.get("name").asString());
        ThreadLocalRandom random = random();
        int chance = random.nextInt(100);
        if ((chance - 30) < 0 && friends.size() > 0) {
            String friend = friends.get(random.nextInt(friends.size()));
            followUser(session, name, friend);
        }
    }

    private void followUser(Session session, String name, String friend) {
        session.run("MATCH (u:User {name:{name}}),(f:User {name:{friend}})" +
                        " CREATE (u)-[:FOLLOWS {time:timestamp()}]->(f)",
                parameters("name", name, "friend", friend)).consume();
    }

    private int readStream(Session session, String name) {
        if (log) System.out.println("Read stream for "+name);
        int count = 0;
        StatementResult result = session.run("MATCH (u:User {name:{name}})-[:STREAM]->(t:Tweet)\n" +
                "RETURN t.text, t.time\n" +
                "ORDER BY t.time DESC LIMIT 10;", parameters("name", name));
        while (result.hasNext()) {
            result.next();
            count++;
        }
        return count;
    }

    private void createTweet(Session session, String user) {
        if (log) System.out.println("Create Tweet for "+user);
        // todo remove older stream tweets (e.g. > 50,100,100) and retrieve them if needed from the followed users themselves
        session.run("MATCH (u:User {name:{name}})\n" +
                        "CREATE (u)-[:TWEETS]->(t:Tweet {text:{text},time:timestamp()})\n" +
                        "WITH (u),(t) " +
                        "MATCH (u)<-[:FOLLOWS]-(f)\n" +
                        "CREATE (f)-[:STREAM]->(t);",
                parameters("name", user, "text", "A Tweet by " + user + " at " + new Date())).consume();
    }
}
