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
(def ^:dynamic *batch-size* 1000)
(def ^:dynamic *upper-key-limit* 1000000)

;;****************************************************************************
;;batch readers/ writers

(defmacro throughput
  [& forms]
  `(let [time-res# (b/bench-collect ~forms)]
     [(/ (second time-res#) 1000.0) (/ (second time-res#) (first  time-res#))]))

(defn write-batch
  "writes a batch of keyvalz to db, returns total kilobytes written"
  [keyvalz]
  (reduce (fn [acc cur]
            (+ acc
               (let [si (tc/size (second cur))]
                 (l/put (first cur) (second cur))
                 si)))
          0 keyvalz))

(defn read-batch
  "reads a batch of keyz from db, returns total kilobytes read"
  [keyz]
  (reduce (fn [acc cur]
            (+ acc
               (let [si (l/get cur)]
                 (if (nil? si) 0 (tc/size si)))))
          0 keyz))

;atoms to hold results
(def write-seq (atom '()))
(def read-seq (atom '()))
(def write-ran (atom '()))
(def read-ran (atom '()))

;;lazy-batches
(defn bench-write-seq [batches]
  (map #(swap! write-seq cons (throughput write-batch %))
       (partition *batch-size* (take (* batches *batch-size*) (tc/kv-seq)))))

(defn bench-read-seq [batches]
  (map #(swap! read-seq cons (throughput read-batch %))
       (partition *batch-size* (take (* batches *batch-size*) tc/keys-seq))))

(defn bench-write-ran [batches limit]
  (map #(throughput write-batch %)
       (partition *batch-size* (take (* batches *batch-size*) (tc/kv-ran limit)))))

(defn bench-read-ran [batches limit]
  (map #(throughput read-batch %)
       (partition *batch-size* (take (* batches *batch-size*) (tc/keys-ran limit)))))


(defn write-seq-result-live [] (second (first @write-seq)))
(defn read-seq-result-live [] (second (first @read-seq)))

(defn sequential-test [batches]
  (c/show (c/time-chart [
                       read-seq-result-live]
                      :title "sequential write and read test"))
;;  (bench-write-seq 100)
  (bench-read-seq 100)  
  )
