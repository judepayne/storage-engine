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

(def  keys-seq
  "infinite lazy sequence of sequentially ordered key strings starting from 1"
  (map #(str key-stub %) (iterate inc 1)))
 
(defn keys-ran
  "infinite lazy sequence of randomly ordered key strings starting from 1
  keys are chosen from the set < limit"
  [limit]
  (repeatedly #(str key-stub (rand-int limit))))
 
(defn valz2
  [randFn]
  (lazy-seq
   (repeatedly
    (fn [] (let [[n payload] (randFn)]
            (str (num->str5 n) payload))))))

(defn rand-val-2-20 [] (rand-val [2 5 10 20]))
(defn rand-val-2-100 [] (rand-val [2 5 10 20 40 67 100]))

; default rand-fn; can be overridden in namesspace using this lib
(def ^:dynamic *rand-fn* rand-val-2-20)

(defn kv-seq []
  "sequence, size s, of sequential keys, random values"
  (map vector keys-seq (valz2 *rand-fn*)))

(defn kv-ran [limit]
  "random sequence of random keys and values where
  keyss are chosen from the set < limit"
  (map vector (keys-ran limit) (valz2 *rand-fn*)))


