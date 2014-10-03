(ns storage-engine.test-common
  (:require [perf-bench :as b]
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

(defn valz
  "given a size and a function which generates random length strings
  returns a tuple/vec where the first is the total size of all generated
  strings and the second is a sequence of the generated strings"
  [size randFn]
  (loop [kilob 0
         coll []
         rem size]
    (if (= rem 0) [kilob coll]
        (let [[n payload] (randFn)]
          (recur
           (+ kilob n)
           (conj coll (str (num->str5 n) payload))
           (dec rem))))))

(def valz-memo (memoize valz))

(def ^:dynamic *rand-fn* rand-val-2-20)

(defn kv-seq [s]
  "sequence, size s, of sequential keys, random values"
  (let [[size valz-] (valz-memo s *rand-fn*)]
    [size
     (into [] (map vector (take s keys-seq) valz-))]))

(defn test [s] (first (kv-seq s)))

(defn kv-ran [s limit]
  "sequence, size s, of random keys, random values
  keys are chosen from the set < limit"
  (let [[size valz-] (valz-memo s *rand-fn*)]
    [size
     (into [] (map vector (take s (keys-ran limit)) valz-))]))
 
