(ns storage-engine.mutable-test
  (:refer-clojure :exclude [get])
  (:require [storage-engine.core :as l]
            [byte-streams :as bs]
            [clojure.edn :as edn]
            [storage-engine.test-common :as tc]
            [live-chart :as c]
            [perf-bench :as b]))

;;agents to collect write and read throughput stats
(def write-throughput (agent {}))
(def read-throughput (agent {}))

;;define batch size and upper key limit
(def ^:dynamic *batch-size* 10)
(def ^:dynamic *upper-key-limit* 1000000)

;;****************************************************************************
;;batch readers/ writers

(defn write-throughput
  "given a size (in kb) of a batch of key vals,
   returns throughput in MBytes per sec"
  [[size keyvals]]
  ;;(println (map #(first %) keyvals))
  (/
   size
   (b/bench (map #(l/put (first %) (second %)) keyvals))))

(defmacro throughput
  [& forms]
  `(let [time-res# (b/bench-collect ~@forms)]
     (/ (second time-res#) (first time-res#))))

(defn write-batch
  "writes a batch of keyvalz to db, returns total kilobytes written"
  [keyvalz]
  (reduce (fn [cur acc]
            (+ acc
               (let [si (tc/size (second cur))]
                 (l/put (first cur) (second cur))
                 si)))
          0 keyvalz))

(defn read-batch
  "reads a batch of keyz from db, returns total kilobytes read"
  [keyz]
  (reduce (fn [cur acc]
            (+ acc
               (tc/size (l/get cur))))
          0 keyz))

;;****************************************************************************

(def ^:dynamic *rand-fn* tc/rand-val-2-20)

;;lazy-batches
(defn bench-read-seq [batch-size]
  (map #(b/bench (read-batch %) 1)
       (partition batch-size tc/keys-seq)))

(defn bench-read-ran [batch-size limit]
  (map #(b/bench (read-batch %) 1)
       (partition batch-size (tc/keys-ran limit))))

(defn bench-write-seq [batches]
  (map #(write-throughput %) (partition batches (tc/kv-seq (* batches *batch-size*)))))

(defn bench-write-ran [batch-size payload limit]
  (map #(b/bench (write-batch %) 1)
       (partition batch-size (tc/kv-ran payload limit))))
