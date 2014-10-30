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
(def write-seq-results (atom 0))
(def read-seq-results (atom 0))
(def write-ran-results (atom 0))
(def read-ran-results (atom 0))

;;lazy-batches
(defn bench-write-seq [batches]
  (map #(do (reset! write-seq-results (throughput write-batch %)))
       (partition *batch-size* (take (* batches *batch-size*) (tc/kv-seq)))))

(defn bench-read-seq [batches]
  (map #(reset! read-seq-results (throughput read-batch %))
       (partition *batch-size* (take (* batches *batch-size*) tc/keys-seq))))

(defn bench-write-ran [batches limit]
  (map #(do (reset! write-ran-results (throughput write-batch %)))
       (partition *batch-size* (take (* batches *batch-size*) (tc/kv-ran limit)))))

(defn bench-read-ran [batches limit]
  (map #(reset! read-ran-results (throughput read-batch %))
       (partition *batch-size* (take (* batches *batch-size*) (tc/keys-ran limit)))))

;;functions to read results from atoms
(defn write-seq-result-live [] @write-seq-results)
(defn read-seq-result-live [] @read-seq-results)
(defn write-ran-result-live [] @write-ran-results)
(defn read-ran-result-live [] @read-ran-results)

(defn sequential-test [batches]
  (c/show (c/time-chart [write-seq-result-live
                         read-seq-result-live])
          :title (str "sequential test: first write ~"
                      (* batches 9 *batch-size* 0.001)
                      " MB then read back 5 times")))

(defn random-test [batches]
  (c/show (c/time-chart [write-ran-result-live
                         read-ran-result-live])
          :title (str "random test: first write ~"
                      (* batches 9 *batch-size* 0.001)
                      " MB then read back " (* batches 9 5 *batch-size* 0.001) " MB")))

;; let's have a macro that makes setting up tests easy
;; specify the sequence of tests to perform using a dsl like this:
;; ws = write sequential, rs = read seq
;; wr = write randomly, rr = read randomly.
;; c = perform the operation concurently
;; some number = number of times operation is to be performed
;; each test in a tuple, e.g. [ws 2] or [rrc 10] or [wr]
;; if the second element is omitted, number of times is 1.
(defn get-op [op]
  (case (subs op 0 2)
              "ws" '(bench-write-seq batches)
              "rs" '(bench-read-seq batches)
              "wr" '(bench-write-ran batches limit)
              "rr" '(bench-read-ran batches limit)
              '(bench-read-seq batches)))

(defn substitute [op]
  (let [times (if (and (= (count op) 2) (number? (second op))) (second op) 1)
        concurrent (if (= (count (first op)) 2) false true )
        op1 (get-op (first op))]
    [times concurrent op1]))

(defmacro perf-test [tests]
  (let [instr# (map (fn [x] (substitute `x)) tests)]
    instr#))

(defn seq-test [batches]
  (reset! read-seq-results 0)
  (reset! write-seq-results 0)
  (sequential-test batches)
  (dorun (bench-write-seq batches))
  (doall (repeatedly 5 #(bench-read-seq batches))))

(defn ran-test [batches limit]
  (reset! read-ran-results 0)
  (reset! write-ran-results 0)
  (random-test batches)
  (dorun (bench-write-ran batches limit))
  (take 5 (repeatedly #(bench-read-ran batches limit))))
