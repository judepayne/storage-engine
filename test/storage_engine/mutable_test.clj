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
     (/ (second time-res#) (first  time-res#))))

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
(def write-seq-results (atom '(0)))
(def read-seq-results (atom '(0)))
(def write-ran-results (atom '(0)))
(def read-ran-results (atom '(0)))

;;lazy-batches
(defn bench-write-seq [batches]
  (map #(do ( swap! write-seq-results conj (throughput write-batch %)))
       (partition *batch-size* (take (* batches *batch-size*) (tc/kv-seq)))))

(defn bench-read-seq [batches]
  (map #(swap! read-seq-results conj (throughput read-batch %))
       (partition *batch-size* (take (* batches *batch-size*) tc/keys-seq))))

(defn write-seq-result-live [] (first @write-seq-results))
(defn read-seq-result-live [] (first @read-seq-results))

(defn sequential-test []
  (c/show (c/time-chart [write-seq-result-live
                         read-seq-result-live])
          :title "sequential write + read test"))


(defn seq-test [batches]
  (reset! read-seq-results '(0))
  (reset! write-seq-results '(0))
  (sequential-test)
 ;;(repeatedly 2 #(bench-write-seq 100))
 ;;(repeatedly 5 #(bench-read-seq 100))
  (dorun (bench-write-seq batches))
  (doall (repeatedly 5 #(bench-read-seq batches))))

