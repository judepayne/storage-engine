(ns storage-engine.test-common.clj
  (:require [perf-bench :as b]
            [storage-engine.core :as l]
            [clojure.string :as string]))


(def alphanumeric "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

(defn random-str
  "Generates a random string <kilo> Kilobytes long"
  [kilo]
  (apply str (repeatedly (* 500 kilo) #(rand-nth alphanumeric))))

(def random-str-memo (memoize random-str))

(def key-stub "swapId-")

(defn rand-val
  [coll] (let [len (rand-nth coll)]
       [len (random-str-memo len)]))

(defn rand-val-2-20 [] (rand-val [2 5 10 20]))
(defn rand-val-2-100 [] (rand-val [2 5 10 20 40 67 100]))

(defn third
  "gets third value in a sequence"
  [coll] (nth coll 2))

(defn num->str5
  [n]
  (format "%05d" n))

(defn str->num
  [s]
  (Integer/parseInt s))
;;****************************************************************************
;;lazy generators
(def keys-seq
  "infinite lazy sequence of sequentially ordered key strings starting from 1"
  (map #(str key-stub %) (iterate inc 1)))
 
(defn keys-ran
  "infinite lazy sequence of randomly ordered key strings starting from 1
  keys are chosen from the set < limit"
  [limit]
  (repeatedly #(str key-stub (rand-int limit))))
 
(defn vals-ran
  "infinite sequence of strings"
  [] (repeatedly rand-val-2-20))

(defn kv-seq []
  "infinite sequence of sequential keys, random values"
  (map (comp flatten vector) keys-seq (vals-ran)))
 
(defn kv-ran [limit]
  "infinite sequence of random keys, random values
  keys are chosen from the set < limit"
  (map (comp flatten vector) (keys-ran limit) (vals-ran)))
 
;;****************************************************************************
;;batch readers/ writers
(defn read-batch
"reads a batch of keyz from db, returns total kilobytes read"
  [this-db keyz]
  (reduce (fn [acc n]
            (+ acc
               (let [v (l/get this-db n)]
                 (str->num (take 5 v)))))
          0 keyz))
 
(defn write-batch
  "writes a batch of keyvalz to db, returns total kilobytes written"
  [this-db keyvals]
  (reduce (fn [acc n]
            (+ acc
               ((l/put this-db (first n) (str (num->str5 (second n)) (third n)))
                (second n))))
          0 keyvals))

(defn write-batch-boo
  "writes a batch of keyvalz to db, returns total kilobytes written"
  [keyvals]
  (reduce (fn [acc n]
            (+ acc
               (second n)))
          0 keyvals))
 
;test
;; type is a lazy-seq
;;(type (write-batch db (kv-seq my-string)))
 
;; writes 1002 k,vs - lighttable chunking??
;(write-batch db (kv-seq my-string))
 
;;type is a lazy-seq
;;(type (take 5 (read-batch db keys-seq)))
 
;;(take 5 (read-batch db keys-seq))
 
;;****************************************************************************
;;lazy-batches
(defn bench-read-seq [this-db batch-size]
  (map #(b/bench (read-batch this-db %) 1)
       (partition batch-size keys-seq)))

(defn bench-read-ran [this-db batch-size limit]
  (map #(b/bench (read-batch this-db %) 1)
       (partition batch-size (keys-ran limit))))

(defn bench-write-seq [this-db batch-size payload]
  (map #(b/bench (write-batch this-db %) 1)
       (partition batch-size (kv-seq payload))))

(defn bench-write-ran [this-db batch-size payload limit]
  (map #(b/bench (write-batch this-db %) 1)
       (partition batch-size (kv-ran payload limit))))
