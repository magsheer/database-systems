package simpledb;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionHandler {
    private static TransactionId FullAccess = new TransactionId();
    private Map<TransactionId, HashSet<PageId>> setMap;
    private final Map<TransactionId, HashSet<TransactionId>> depGraph;
    private Map<PageId, Object> locks;
    private Map<PageId, HashSet<TransactionId>> locksS;
    private Map<PageId, TransactionId> locksX;


    public TransactionHandler() {
        this.locks = new ConcurrentHashMap<>();
        this.locksS = new ConcurrentHashMap<>();
        this.setMap = new ConcurrentHashMap<>();
        this.depGraph = new ConcurrentHashMap<>();
        this.locksX = new ConcurrentHashMap<>();
    }

    private Object getLock(PageId pid) {
        if (!(locks.containsKey(pid))) {
            locks.put(pid, new Object());
            locksS.put(pid, new HashSet<>());
            locksX.put(pid, FullAccess);
        }

        return locks.get(pid);
    }

    private boolean hasLoop(TransactionId tid) {
        HashSet<TransactionId> seen = new HashSet<>();
        LinkedList<TransactionId> bfs = new LinkedList<>();

        bfs.add(tid);

        while (!(bfs.isEmpty())) {
            TransactionId recent = bfs.remove();
            if (seen.contains(recent)) {
                return true;
            }

            seen.add(recent);

            if (depGraph.containsKey(recent) && !(this.depGraph.get(recent).isEmpty())) {
                bfs.addAll(depGraph.get(recent));
            }
        }

        return false;
    }

    public void getHandle(TransactionId tid, PageId pid, Permissions p)
            throws TransactionAbortedException {

        if (!(depGraph.containsKey(tid))) {
            depGraph.put(tid, new HashSet<>());
        }

        Object access = this.getLock(pid);
        if ((p == Permissions.READ_ONLY) &&
                !(locksS.get(pid).contains(tid))) {
            while (true) {
                synchronized (access) {
                    if (locksX.get(pid).equals(FullAccess) ||
                            locksX.get(pid).equals(tid)) {
                        synchronized (locksS.get(pid)) {
                            locksS.get(pid).add(tid);
                        }

                        synchronized (depGraph) {
                            depGraph.remove(tid);
                        }

                        break;
                    }

                    synchronized (depGraph) {
                        if (depGraph
                                .get(tid)
                                .add(locksX.get(pid))) {
                            if (hasLoop(tid)) {
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                }
            }
        } else if ((p == Permissions.READ_WRITE) &&
                !(locksX.get(pid).equals(tid))) {
            while (true) {
                synchronized (access) {

                    HashSet<TransactionId> deps = new HashSet<>();
                    if (!(locksX.get(pid).equals(FullAccess))) {
                        deps.add(locksX.get(pid));
                    }

                    synchronized (locksS.get(pid)) {
                        for (TransactionId t : this.locksS.get(pid)) {
                            if (!(t.equals(tid))) {
                                deps.add(t);
                            }
                        }
                    }


                    if (deps.isEmpty()) {
                        synchronized (locksS.get(pid)) {
                            locksS.get(pid).remove(tid);
                        }

                        locksX.put(pid, tid);

                        synchronized (depGraph) {
                            depGraph.remove(tid);
                        }

                        break;
                    }

                    synchronized (depGraph) {
                        if (depGraph
                                .get(tid)
                                .add(this.locksX.get(pid)) ||
                                depGraph.get(tid).addAll(deps)) {

                            if (hasLoop(tid)) {
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                }
            }
        }

        if (!(setMap.containsKey(tid))) {
            setMap.put(tid, new HashSet<>());
        }

        synchronized (setMap.get(tid)) {
            setMap.get(tid).add(pid);
        }
    }

    public void removeHandle(TransactionId tid, PageId pid) {
        if (!(setMap.containsKey(tid))) {
            return;
        }

        Object access = this.getLock(pid);
        synchronized (access) {
            if (locksX.get(pid).equals(tid)) {
                locksX.put(pid, FullAccess);
            }

            synchronized (locksS.get(pid)) {
                locksS.get(pid).remove(tid);
            }
        }

        synchronized (setMap.get(tid)) {
            setMap.get(tid).remove(pid);
        }
    }

    public void removeAllHandles(TransactionId tid) {
        if (!(setMap.containsKey(tid))) {
            return;
        }

        Iterator<PageId> pageIdIterator = this.iterateoverset(tid);
        while (pageIdIterator.hasNext()) {
            PageId pid = pageIdIterator.next();

            Object access = this.getLock(pid);
            synchronized (access) {
                if (locksX.get(pid).equals(tid)) {
                    locksX.put(pid, FullAccess);
                }

                synchronized (locksS.get(pid)) {
                    locksS.get(pid).remove(tid);
                }
            }
        }

        this.setMap.remove(tid);
    }

    public Iterator<PageId> iterateoverset(TransactionId tid) {
        if (!(setMap.containsKey(tid))) {
            return null;
        }

        return setMap.get(tid).iterator();
    }

    public boolean checkForHandles(TransactionId tid, PageId pid) {
        if (!(setMap.containsKey(tid))) {
            return false;
        }

        synchronized (setMap.get(tid)) {
            return setMap.get(tid).contains(pid);
        }
    }
}