(ns storage-engine.mutable-test
  (:refer-clojure :exclude [get])
  (:require [clojure.test :refer :all]
            [storage-engine.core :as l]
            [storage-engine.test-common :as tc]
            [live-chart :as c]
            [perf-bench :as b]))

;;agents to collect write and read throughput stats
(def write-throughput (agent {}))
(def read-throughput (agent {}))

;;define batch size and upper key limit
(def ^:dynamic *batch-size* 1000)
(def ^:dynamic *upper-key-limit* 1000000)

;;****************************************************************************
;;batch readers/ writers
 
(defn write-batch
  "writes a batch of keyvalz to db"
  [this-db keyvals]
  (map #(l/put this-db (first %) (second %)) keyvals))

(defn read-batch
  "reads a batch of keyz from db, returns total kilobytes read"
  [this-db keyz]
  (reduce (fn [acc n]
            (+ acc
               (let [v (l/get this-db n)]
                 (tc/str->num (take 5 v)))))
          0 keyz))
 
;;****************************************************************************


;;lazy-batches
(defn bench-read-seq [this-db batch-size]
  (map #(b/bench (read-batch this-db %) 1)
       (partition batch-size tc/keys-seq)))

(defn bench-read-ran [this-db batch-size limit]
  (map #(b/bench (read-batch this-db %) 1)
       (partition batch-size (tc/keys-ran limit))))

(defn bench-write-seq [this-db batch-size payload]
  (map #(b/bench (write-batch this-db %) 1)
       (partition batch-size (tc/kv-seq payload))))

(defn bench-write-ran [this-db batch-size payload limit]
  (map #(b/bench (write-batch this-db %) 1)
       (partition batch-size (tc/kv-ran payload limit))))
