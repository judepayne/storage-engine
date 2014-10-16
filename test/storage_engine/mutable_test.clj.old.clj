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
(def ^:dynamic *batch-size* 10)
(def ^:dynamic *upper-key-limit* 1000000)

;;****************************************************************************
;;batch readers/ writers
 
(defn write-batch
  "writes a batch of keyvalz to db"
  [keyvals]
  (map #(l/put (first %) (second %)) keyvals))
;;---> Change starts here

(defn write-throughput
  "given a size (in kb) of a batch of key vals,
   returns throughput in MBytes per sec"
  [[size keyvals]]
  ;;(println (map #(first %) keyvals))
  (/
   size
   (b/bench (map #(l/put (first %) (second %)) keyvals))))

(defn read-batch
  "reads a batch of keyz from db, returns total kilobytes read"
  [keyz]
  (reduce (fn [acc n]
            (+ acc
               (let [v (l/get n)]
                 (tc/str->num (take 5 v)))))
          0 keyz))
 
;;****************************************************************************

(def *rand-fn* tc/rand-val-2-20)

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


;;****************************************************************************
;; let's run a test

;; (l/startup)


