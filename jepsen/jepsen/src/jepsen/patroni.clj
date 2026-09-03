(ns jepsen.patroni
  "Tests for Patroni"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.string :as string]
            [jepsen [tests :as tests]
                    [os :as os]
                    [db :as db]
                    [client :as client]
                    [control :as control]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [checker :as checker]
                    [util :as util :refer [timeout]]
                    [net :as net]]
            [knossos [op :as op]]
            [clojure.java.jdbc :as j]))

(def register (atom 0))

(def default-seed 17)
(def default-time-limit 7200)
(def default-final-time-limit 600)
(def default-recovery-seconds 60)
(def primary-probe-sql
  "BEGIN; CREATE TEMP TABLE patroni_primary_probe(value integer); SELECT pg_sleep(1); ROLLBACK")

(defn env-long [name fallback]
  (Long/parseLong (or (System/getenv name) (str fallback))))

(def test-seed (env-long "JEPSEN_SEED" default-seed))
(def test-random (java.util.Random. test-seed))
(def test-time-limit (env-long "JEPSEN_TIME_LIMIT" default-time-limit))
(def final-time-limit (env-long "JEPSEN_FINAL_TIME_LIMIT" default-final-time-limit))
(def recovery-seconds (env-long "JEPSEN_RECOVERY_SECONDS" default-recovery-seconds))

(defn choose [values]
  (let [items (vec values)]
    (locking test-random
      (nth items (.nextInt test-random (count items))))))

(defn patroni-node [test]
  (choose (filter #(string/includes? (name %) "patroni") (:nodes test))))

(defn patroni-nodes [test]
  (filter #(string/includes? (name %) "patroni") (:nodes test)))

(defn open-conn
  "Given a JDBC connection spec, opens a new connection unless one already
  exists. JDBC represents open connections as a map with a :connection key.
  Won't open if a connection is already open."
  [spec]
  (if (:connection spec)
    spec
    (j/add-connection spec (j/get-connection spec))))

(defn close-conn
  "Given a spec with JDBC connection, closes connection and returns the spec w/o connection."
  [spec]
  (when-let [conn (:connection spec)]
    (.close conn))
  {:classname   (:classname spec)
   :subprotocol (:subprotocol spec)
   :subname     (:subname spec)
   :user        (:user spec)
   :password    (:password spec)})

(defmacro with-conn
  "This macro takes that atom and binds a connection for the duration of
  its body, automatically reconnecting on any
  exception."
  [[conn-sym conn-atom] & body]
  `(let [~conn-sym (locking ~conn-atom
                     (swap! ~conn-atom open-conn))]
     (try
       ~@body
       (catch Throwable t#
         (locking ~conn-atom
           (swap! ~conn-atom (comp open-conn close-conn)))
         (throw t#)))))

(defn conn-spec
  "Return postgresql connection spec for given node name"
  [node]
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname     (str "//" (name node) ":5432/postgres?prepareThreshold=0")
   :user        "postgres"
   :password    "postgres"})

(defn noop-client
  "Noop client"
  []
  (reify client/Client
    (setup! [_ test]
      (info "noop-client setup"))
    (invoke! [this test op]
      (assoc op :type :info, :error "noop"))
    (close! [_ test])
    (teardown! [_ test] (info "teardown"))
    client/Reusable
    (reusable? [_ test] true)))

(defn pg-client
  "PostgreSQL client"
  [conn]
  (reify client/Client
    (setup! [_ test]
      (info "pg-client setup"))
    (open! [_ test node]
      (let [conn (atom (conn-spec node))]
        (cond (string/includes? (name node) "patroni")
              (pg-client conn)
              true
              (noop-client))))
    (invoke! [this test op]
      (try
          (timeout 5000 (assoc op :type :info, :error "timeout")
            (with-conn [c conn]
              (case (:f op)
                :read (assoc op :type :ok,
                                :value (->> (j/query c ["select value from set for update"]
                                                     {:row-fn :value})
                                            (vec)
                                            (set)))
                :add (do (j/execute! c [(str "insert into set values ("
                                                (get op :value) ")")])
                            (assoc op :type :ok)))))
        (catch Throwable t#
          (let [m# (.getMessage t#)]
            (cond (re-find #"ERROR: cannot execute .* in a read-only transaction" m#)
                  (assoc op :type :info, :error "read-only")
                  true
                  (assoc op :type :info, :error m#))))))
    (close! [_ test] (close-conn conn))
    (teardown! [_ test])
    client/Reusable
    (reusable? [_ test] true)))

(defn db
  "PostgreSQL database"
  []
  (reify db/DB
    (setup! [_ test node]
      (info (str (name node) " setup")))

    (teardown! [_ test node]
      (info (str (name node) " teardown")))))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn a [_ _] {:type :invoke, :f :add, :value (swap! register (fn [current-state] (+ current-state 1)))})

(def patroni-set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify checker/Checker
    (check [this test history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"}

          (let [; The OK set is every read value which we tried to add
                ok          (set/intersection final-read attempts)

                ; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.
                recovered   (set/difference ok adds)]

            {:valid?          (and (empty? lost) (empty? unexpected))
             :ok              (util/integer-interval-set-str ok)
             :lost            (util/integer-interval-set-str lost)
             :unexpected      (util/integer-interval-set-str unexpected)
             :recovered       (util/integer-interval-set-str recovered)
             :ok-frac         (util/fraction (count ok) (count attempts))
             :unexpected-frac (util/fraction (count unexpected) (count attempts))
             :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))

(defn- overlap? [left right]
  (< (max (:start left) (:start right))
     (min (:end left) (:end right))))

(def primary-overlap
  "Reject concurrent successful write probes on different nodes."
  (reify checker/Checker
    (check [this test history opts]
      (let [overlaps (for [event history
                           :when (and (= :probe-primary (:f event))
                                      (= :info (:type event)))
                           left (:value event)
                           right (:value event)
                           :when (neg? (compare (:node left) (:node right)))
                           :when (overlap? left right)]
                       [(:node left) (:node right)])]
        {:valid? (empty? overlaps)
         :overlaps (vec overlaps)}))))

(defn- probe-node [node]
  (let [start (System/nanoTime)]
    (try
      (control/on node
        (control/exec :timeout :4 :psql :-U :postgres :-v :ON_ERROR_STOP=1
                      :-Atqc primary-probe-sql))
      {:node (name node) :start start :end (System/nanoTime)}
      (catch Throwable t#
        nil))))

(defn- probe-primaries [test]
  ; A successful one-second transaction proves the node stayed writable.
  (->> (patroni-nodes test)
       (pmap probe-node)
       (filter some?)
       (vec)))

(defn- try-exec [& command]
  (try
    (apply control/exec command)
    (catch Throwable t#
      (debug (str "Process command had no target: " (.getMessage t#)))
      nil)))

(defn apply-process-fault [node fault]
  (control/on node
    (case fault
      :kill-controller
        (try-exec :pkill :-9 :-f "patroni.controller")
      :kill-agent
        (do (try-exec :pkill :-9 :-x "postgres")
            (try-exec :pkill :-9 :-f "patroni.agent"))
      :kill-postgres
        (try-exec :pkill :-9 :-x "postgres")
      :pause-controller
        (try-exec :pkill :-STOP :-f "patroni.controller")
      :pause-agent
        (do (try-exec :pkill :-STOP :-x "postgres")
            (try-exec :pkill :-STOP :-f "patroni.agent"))
      :pause-postgres
        (try-exec :pkill :-STOP :-x "postgres")
      :drop-socket
        (do (try-exec :rm :-f "/run/patroni/agent.sock")
            (try-exec :pkill :-9 :-x "postgres")
            (try-exec :pkill :-9 :-f "patroni.agent"))
      :restart-pod
        (do (try-exec :pkill :-9 :-f "patroni.controller")
            (try-exec :pkill :-9 :-x "postgres")
            (try-exec :pkill :-9 :-f "patroni.agent"))
      :resume-processes
        (do (try-exec :pkill :-CONT :-f "patroni.controller")
            (try-exec :pkill :-CONT :-x "postgres")
            (try-exec :pkill :-CONT :-f "patroni.agent")))))

(defn process-faults
  "Fault controller, agent-container, PostgreSQL, and socket boundaries."
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)
    (invoke! [this test op]
      (if (= :probe-primary (:f op))
        (assoc op :value (probe-primaries test))
        (assoc op :value
          (try
            (let [fault (:f op)
                  nodes (if (= fault :resume-processes)
                          (patroni-nodes test)
                          [(patroni-node test)])]
              (doseq [node nodes]
                (apply-process-fault node fault))
              [fault :on nodes])
            (catch Throwable t#
              (let [message (.getMessage t#)]
                (warn (str "Unable to apply process fault: " message))
                message))))))
    (teardown! [this test]
      (doseq [node (patroni-nodes test)]
        (apply-process-fault node :resume-processes))
      (info "Stopping process faults"))
    nemesis/Reflection
    (fs [this] #{})))

(defn switcher
  "Executes switchover"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)
    (invoke! [this test op]
             (case (:f op)
               :switch (assoc op :value
                          (try
                              (let [node (patroni-node test)]
                                (control/on node
                                  (control/exec :timeout :10 :patronictl :switchover :--force))
                                (assoc op :value [:switchover :on node]))
                            (catch Throwable t#
                              (let [m# (.getMessage t#)]
                                (do (warn (str "Unable to run switch: "
                                               m#))
                                    m#)))))))
    (teardown! [this test]
      (info (str "Stopping switcher")))
    nemesis/Reflection
    (fs [this] #{})))

(def nemesis-starts
  [:start-halves
   :start-ring
   :start-one
   :switch
   :kill-controller
   :kill-agent
   :kill-postgres
   :pause-controller
   :pause-agent
   :pause-postgres
   :drop-socket
   :restart-pod])

(defn patroni-test
  [patroni-nodes etcd-nodes]
  {:nodes     (concat patroni-nodes etcd-nodes)
   :name      "patroni"
   :os        os/noop
   :db        (db)
   :ssh       {:private-key-path "/root/.ssh/id_rsa"}
   :net       net/iptables
   :client    (pg-client nil)
   :nemesis   (nemesis/compose {{:start-halves :start} (nemesis/partition-random-halves)
                                {:start-ring   :start} (nemesis/partition-majorities-ring)
                                {:start-one    :start
                                 ; All partitioners heal all nodes on stop so we define stop once
                                 :stop         :stop} (nemesis/partition-random-node)
                                #{:switch} (switcher)
                                #{:kill-controller :kill-agent :kill-postgres
                                  :pause-controller :pause-agent :pause-postgres
                                  :drop-socket :restart-pod :resume-processes
                                  :probe-primary} (process-faults)})
   :generator (gen/phases
                (->> a
                     (gen/stagger 1/50)
                     (gen/nemesis
                       (fn [] (map gen/once
                                    [{:type :info, :f (choose nemesis-starts)}
                                     {:type :info, :f (choose nemesis-starts)}
                                     {:type :info, :f :probe-primary}
                                     {:type :sleep, :value recovery-seconds}
                                     {:type :info, :f :probe-primary}
                                     {:type :info, :f :resume-processes}
                                     {:type :info, :f :stop}
                                     {:type :sleep, :value recovery-seconds}])))
                     (gen/time-limit test-time-limit))
                (->> r
                     (gen/stagger 1)
                     (gen/nemesis
                       (fn [] (map gen/once
                                    [{:type :info, :f :stop}
                                     {:type :info, :f :resume-processes}
                                     {:type :info, :f :probe-primary}
                                     {:type :sleep, :value recovery-seconds}])))
                     (gen/time-limit final-time-limit)))
   :checker   (checker/compose {:history patroni-set
                                :writable-primary primary-overlap})
   :remote    control/ssh})
