(ns storage-engine.test-common
  (:require [perf-bench :as b]
            [clojure.string :as string]))


(def ^{:private true} alphanumeric "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

(defn- random-str
  "Generates a random string <kilo> Kilobytes long"
  [kilo]
  (apply str (repeatedly (* 500 kilo) #(rand-nth alphanumeric))))

(def ^{:private true} random-str-memo (memoize random-str))

(def ^{:private true} key-stub "swapId-")

(defn- rand-val
  [coll] (let [len (rand-nth coll)]
       [len (random-str-memo len)]))

(defn- third
  "gets third value in a sequence"
  [coll] (nth coll 2))

;;****************************************************************************
;;lazy generators. Public functions

(defn num->str5
  [n]
  (format "%05d" n))

(defn str->num
  [s]
  (Integer/parseInt s))

(defn size
  "reads the first 5 chars of a size-tagged string
  returns that size as a long"
  [s]
  (str->num (subs s 0 5)))

(defn total-size
  "calculates total size of a size-tagged seq of strings"
  [coll]
  (reduce
   (fn [acc cur]
     (+ acc
        (size cur)))
   0 coll))

(def  keys-seq
  "infinite lazy sequence of sequentially ordered key strings starting from 1"
  (map #(str key-stub %) (iterate inc 1)))
 
(defn keys-ran
  "infinite lazy sequence of randomly ordered key strings starting from 1
  keys are chosen from the set < limit"
  [limit]
  (repeatedly #(str key-stub (rand-int limit))))
 
(defn valz
  [randFn]
  (repeatedly
   (fn [] (let [[n payload] (randFn)]
           (str (num->str5 n) payload)))))

(def valz-m
  "memoize valz to eliminate time generating random strings
  on repeated calls"
  (memoize valz))

(defn rand-val-2-20 [] (rand-val [2 5 10 20]))
(defn rand-val-2-100 [] (rand-val [2 5 10 20 40 67 100]))

; default rand-fn; can be overridden in namesspace using this lib
(def ^:dynamic *rand-fn* rand-val-2-20)

(defn kv-seq []
  "sequence, size s, of sequential keys, random values"
  (map vector keys-seq (valz-m *rand-fn*)))

(defn kv-ran [limit]
  "random sequence of random keys and values where
  keyss are chosen from the set < limit"
  (map vector (keys-ran limit) (valz-m *rand-fn*)))

;;*********************************************************************
;;simpler generation to test throughput figures obtained (seem too
;;low)

(def payload (random-str 2))
(defn kv-seq-simple []
  (map vector keys-seq (repeat (str "00002"  payload))))

