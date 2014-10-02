(ns storage-engine.core
  (:refer-clojure :exclude [get])
  (:require [clojure.edn :as edn]
            [byte-streams :as bs]
            [clojure.java.io :as io]
            [storage-engine.kvstore :as kv]
            [storage-engine.utils :as util]
            [clojure.core.async :as async]))


;;configuartion
(def ^{:private true} start-config (atom {}))
(defn- config [] (let [c @start-config]
                   (if (not (empty? c))
                     c
                     (edn/read-string (slurp "storage-engine.core.config.edn")))))

;;atom to hold database/s/ namespaces (see design)
(def ^{:private true} snaps (agent {}))
(def ^{:private true} curr-db (agent nil))

;;file utility functions
(defn- list-subdirs
  "creates list of sub-directory names matching pattern in the given directory"
  [parent-dir pattern]
  (->> (file-seq (io/file parent-dir))
       (filter #(.isDirectory %))
       (map #(.getName %))
       (filter #(not (nil? (re-seq pattern %))))))

(defn- dbdir-to-key [pattern dir]
  (keyword (second (clojure.string/split dir pattern))))

;;START LvlDB specific stuff
;; from config
(def ^{:private true} store-dir ((config) :db-dir))
(def ^{:private true} snap-db-pattern (read-string ((config) :minor-db-pattern)))
(def ^{:private true} current-db ((config) :current-db))
(def ^{:private true} current-db-options (eval ((config) :current-db-options)))
(def ^{:private true} snap-db-options (eval ((config) :minor-db-options)))
(defn to-snap-name [n] (str snap-db-pattern n))
(def ^{:private true} alive? (atom false))

;; Wrap lvldb specific openning functions generically
;; This section would need rewriting for a db different to leveldb
(defn closed? [x] (not (:open? x)))
(defn open? [x] (:open? x))

(defn- open-db-
  "open a single lvlDB from either a connection & options or a closed lvlDB"
  ([conn options] (kv/open-lvldb conn options))
  ([db] (if (vector? db) (apply open-db- db)
          (if (closed? db)
            (kv/open-lvldb (:conn db) (:options db))
            db))))

(defn- close-db-
  "closes single( lvlDB. performs check to see if already closed"
  [db]
  (if (open? db)
    (kv/close db)
    db))

;;END LvlDB specific stuff

;;*********************OPEN/ CLOSE DB FUNCTIONS***********************
;;************General open/close current & snap functions*************

(defn- snap-map []
  (let [conns (list-subdirs store-dir snap-db-pattern)]
    (zipmap
     (map #(dbdir-to-key snap-db-pattern %) conns)
     (map
      #(vector (str store-dir "/" %) snap-db-options)
      conns))))

(defn- create-snap-db- [conn]
  (let [snap-pat (to-snap-name conn)]
    (if-not ((keyword snap-pat) @snaps)
      (kv/open-lvldb (str store-dir "/" snap-pat) snap-db-options)
      (throw (RuntimeException. "snap already exists")))))


;*********************open/ close (agent) functions*******************
(defn- open-current-db
  "open the current db if not already open"
  []
  (send
    curr-db
    (fn [curr]
      (if (nil? (:open? curr))
        (open-db- (str store-dir "/" current-db) current-db-options)
        (open-db- curr)))))

(defn- close-current-db
  "close the current db if not already closed"
  []
  (send
    curr-db
    (fn [curr] (close-db- curr))))

(defn- open-snaps
  "open all snap dbs in config store dir"
  []
  (send
     snaps
     (fn [m] (util/converge-to open-db- open? m (snap-map)))))

(defn- close-snaps
  "close all (open) snaps"
  []
  (send
     snaps
     (fn [m] (util/map-vals #(close-db- %) m))))

(defn- close-snap
  "close snap with supplied name"
  [tag]
  (send
     snaps
     (fn [m] (assoc m tag (close-db- (tag m))))))

(defn- open-snap
  "open snap with supplied name"
  [tag]
  (send
     snaps
     (fn [m] (assoc m tag (open-db- (tag m))))))

(defn- create-snap
  "create snap with supplied name
  converts name to keyword and adds snap prefix"
  [tag]
  (let [db-name tag]
    (send
      snaps
      (fn [m] (assoc m
              (keyword db-name)
              (create-snap-db- db-name))))))

(defn- snap-status []
  (util/map-vals
     #(:open? %)
   @snaps))


;****************************(info) usage*****************************
;; @snaps
;; (restart-agent snaps {})
;; (open-all-snaps)
;; (ensure-all-snaps-open)
;; (close-all-snaps)
;; (close-snap :EOD-05AUG14)
;; (open-snap :EOD-05AUG14)
;; (snap-status)
;*********************************************************************
;*********************************************************************
;*******************startup/ shutdown functions***********************
(defn startup
  ([]
     (try
       (await snaps)
       (open-snaps)
       (reset! alive? true)
       (await snaps)
       (close-snaps)
       (open-current-db)
       (catch Exception e
         (str "caught exception: " (.getCause e)
              (.getMessage e)))))
  ([config]
     (reset! start-config config)
     (startup)))

(defn shutdown []
  (try
    (close-snaps)
    (reset! alive? false)
    (send
     curr-db
     (fn [_] (kv/close @curr-db)))
    (catch Exception e
      (str "caught exception: " (.getCause e)
           (.getMessage e)))))


;***********************public clojure api****************************
;********************private api helper fns***************************

(defn- ->snap-db
  [snap-db]
  (map
   #(let [[k v] %]
      (kv/put snap-db k v))
   (with-open [snap (kv/snapshot @curr-db)]
    (kv/iterator snap))))


;**********************public clojure api fns*************************
(defn get
  "returns value for the specified key"
  [k]
  (kv/get @curr-db k))

(defn put
  "puts a key value pair into the current db"
  [k v]
  (kv/put @curr-db k v))

(defn get-current
  "maps the supplied fn over lazy-seq representing the current state
   results in a new lazy-seq with elements of form [k v]"
  [f]
  (map f
       (with-open [snap (kv/snapshot @curr-db)]
         (kv/iterator snap))))

(defn get-current-async
  "delivers the current state into supplied core-async channel"
  [channel]
  (async/thread
   (do
     (doseq [i (with-open [snap (kv/snapshot @curr-db)]
                 (kv/iterator snap))]
       (async/>!! channel i))
     (async/close! channel))))

(comment
;;unit test for get-current-async/ get-snap-async
(startup)
(def c (async/chan 10))

(async/thread
 (loop []
   (let [k (async/<!! c)]
     (if-not k (println "finished")
       (do
         (println k)
         (recur))))))

(get-current c)
)

(defn snap-to
  "copy the state of the current db into a new named snap"
  [snap-name]
  (do
    (create-snap snap-name)
    (await snaps)
    (let [db ((keyword snap-name) @snaps)]
      (->snap-db db))))

(defn get-snap
  "maps the supplied fn over lazy-seq representing the snap
   results in a new lazy-seq with elements of form [k v]"
  [snap-name f]
  (do
    (open-snap (to-snap-name snap-name))
    (await snaps)
    (let [db ((keyword snap-name) @snaps)]
      (map f (kv/iterator db)))))

(defn get-snap-async
  "delivers the named snap into supplied core.async channel"
  [snap-name channel]
  (do
    (open-snap (to-snap-name snap-name))
    (await snaps)
    (let [db ((keyword snap-name) @snaps)]
        (async/thread
         (do
           (doseq [i (kv/iterator db)]
             (async/>!! channel i))
           (async/close! channel))))))

(defn is-alive? []
  @alive?)

(defn list-snaps []
  (map name (keys @snaps)))
;; all meta data?


