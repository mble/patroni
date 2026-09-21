(ns jepsen.patroni-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.patroni :as patroni]))

(def patroni_nodes ["patroni1" "patroni2" "patroni3"])

(def etcd_nodes ["etcd1" "etcd2" "etcd3"])

(deftest absent-process-is-noop
  (with-redefs [jepsen.control/exec (fn [& command]
                                      (throw (ex-info "absent" {:command command})))]
    (is (nil? ((ns-resolve 'jepsen.patroni 'try-exec) :pkill :-CONT :-x "postgres")))))

(deftest overlapping-writable-primaries-fail
  (let [check (deref (ns-resolve 'jepsen.patroni 'primary-overlap))
        history [{:type :info
                  :f :probe-primary
                  :value [{:node "patroni1" :start 10 :end 30}
                          {:node "patroni2" :start 20 :end 40}]}]]
    (is (false? (:valid? (jepsen.checker/check check {} history {}))))))

(deftest fault-cycle-is-pure
  (let [fault-events (ns-resolve 'jepsen.patroni 'fault-events)
        required (set patroni/nemesis-starts)
        faults (fn [] (->> (fault-events)
                           (map :f)
                           (filter required)
                           (vec)))]
    (is (= patroni/nemesis-starts (faults)))
    (is (= (faults) (faults)))))

(deftest missing-fault-coverage-fails
  (let [coverage @(ns-resolve 'jepsen.patroni 'fault-coverage)
        complete (map-indexed (fn [index fault]
                                {:index index, :type :info, :f fault})
                              patroni/nemesis-starts)
        history (map-indexed (fn [index fault]
                               {:index index, :type :info, :f fault})
                             (rest patroni/nemesis-starts))
        result (jepsen.checker/check coverage {} history {})]
    (is (:valid? (jepsen.checker/check coverage {} complete {})))
    (is (false? (:valid? result)))
    (is (= [:start-halves] (:missing result)))))

(deftest patroni-test
  (is (:valid? (:results (jepsen/run! (patroni/patroni-test patroni_nodes etcd_nodes))))))
