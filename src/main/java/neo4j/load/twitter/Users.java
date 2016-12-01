package neo4j.load.twitter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author mh
 * @since 30.11.16
 */
class Users {
    private final ConcurrentHashMap<String, String> bookmarks = new ConcurrentHashMap<>();
    private final List<String> names = new CopyOnWriteArrayList<>();

    String randomUser() {
        return names.get(random().nextInt(names.size()));
    }

    String bookmark(String user, String bookmark) {
        return bookmark != null ? bookmarks.put(user, bookmark) : bookmarks.get(user);
    }

    String bookmarkFor(String user) {
        return bookmarks.get(user);
    }

    Collection<String> randomUsers(int count) {
        ThreadLocalRandom random = random();
        Set<String> result = new HashSet<>(count);
        int max = names.size();
        for (int i = 0; i < count; i++) {
            result.add(names.get(random.nextInt(max)));
        }
        return result;
    }

    private ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }

    String newName() {
        String name = UUID.randomUUID().toString();
        names.add(name);
        return name;
    }

    void addAll(List<String> names) {
        this.names.addAll(names);
    }

    int size() {
        return names.size();
    }
}
