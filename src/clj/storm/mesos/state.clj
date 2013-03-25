(ns storm.mesos.state
  (:require [backtype.storm.cluster :as c])
  (:require [backtype.storm.zookeeper :as zk])
  (:require [clojure.set :as set])
  (:use [backtype.storm.config :only [read-storm-config]])
  (:use [backtype.storm bootstrap util])
  (:import [backtype.storm.scheduler TopologyDetails]
           [java.util Map]
           [storm.mesos IMesosNimbusStateHelper]
           [storm.mesos MesosNimbus]))

(bootstrap)

(defn cluster-state-with-new-root [cst sub-root]
  (let [sub-root (if (.startsWith sub-root "/") sub-root (str "/" sub-root))]
    (c/mkdirs cst sub-root)
    (reify
      backtype.storm.cluster.ClusterState
      (set-data [this path data] (c/set-data cst (str sub-root "/" path) data))
      (get-data [this path w?] (c/get-data cst (str sub-root "/" path) w?))
      (get-children [this path w?] (c/get-children cst (str sub-root "/" path) w?))
      (delete-node [this path] (c/delete-node cst (str sub-root "/" path))))))

(defn make-fn-newly-launched-topo-name->executor-uri [conf]
  (let [regex->uri (doall (->> (or (conf MesosNimbus/CONF_EXECUTOR_URI_OVERRIDES) [])
                               (map-key re-pattern)))]
    (fn [name]
      (loop [regex->uri regex->uri]
        (if-let [[re uri] (first regex->uri)]
          (if (re-matches re name)
            uri
            (recur (rest regex->uri)))
          (conf MesosNimbus/CONF_EXECUTOR_URI_DEFAULT))))))

(defn empty-str-as-nil [^String s] (if (and s (= "" (.trim s))) nil s))

(defn mk-mesos-nimbus-state-helper [conf cluster-state]
  (let [mesos-st (cluster-state-with-new-root cluster-state "/mesos-nimbus/executor-uri")
        storm-st (c/mk-storm-cluster-state cluster-state)
        topo-name->uri (make-fn-newly-launched-topo-name->executor-uri conf)
        timer (mk-halting-timer)

        state-get-uri! (fn [topo-id] (if-let [data (c/get-data mesos-st topo-id false)]
                                       (String. data "UTF-8")))
        state-set-uri! (fn [topo-id uri] (c/set-data mesos-st topo-id (.getBytes uri "UTF-8")) uri)]
    (reify
      IMesosNimbusStateHelper
      (init [this conf]        
        ;; The first time you upgrade your cluster to this code, this will write out mesos URIs for every topo id.
        (doseq [[topo-id name] (->> (set/difference (set (c/active-storms storm-st)) (set (c/get-children mesos-st "/" false)))
                                    (map (juxt identity (fn [id] (:storm-name (c/storm-base storm-st id false))))))]
          (state-set-uri! topo-id (topo-name->uri name)))
        ;; Garbage collect topologies that were killed.
        (let [GC-fn (fn []
                      (log-message "Doing MesosNimbus garbage collection...")
                      (doseq [garbage (set/difference (set (c/get-children mesos-st "/" false)) (set (c/active-storms storm-st)))]
                        (log-message "Cleaning up MesosNimbus state: " garbage)
                        (c/delete-node mesos-st garbage)))]
          (schedule-recurring timer 0 (* 5 60) GC-fn)))

      ;; Always return the same URI for the same topology id.
      ;;  If it's a newly launched topology, use config to get default, or overriden URI.
      (^String getExecutorURI [this ^TopologyDetails details]
        (if-let [uri (empty-str-as-nil (state-get-uri! (.getId details)))]
          uri
          (let [uri (topo-name->uri (.getName details))]
            (log-message "Newly launched topology with name " (.getName details) " will use mesos executor uri: " uri)
            (state-set-uri! (.getId details) uri)))))))
