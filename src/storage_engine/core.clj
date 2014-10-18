(ns storage-engine.core
  (:refer-clojure :exclude [get])
  (:require [clojure.edn :as edn]
            [byte-streams :as bs]
            [clojure.java.io :as io]
            [storage-engine.kvstore :as kv]
            [storage-engine.utils :as util]
            [clojure.core.async :as async]))


;;Atoms for holding STATE
;;holds snap dbs
(def  snaps (atom {}))
;;holds current db
(def  curr-db (atom nil))
;;holds config
(def  config (atom {}))

;;configuartion handling
(def config-file "config/mutable-config.edn")

(defn- get-file-config []
  (try (edn/read-string (slurp config-file))
       (catch Exception e nil)))

(defn- set-config!
  "load default config from file"
  ([conf]
     (let [file-conf (get-file-config)
           final-conf (if (nil? file-conf) conf
                          (merge file-conf conf))]
       (reset! config final-conf)))
  ([]
     (let [file-conf (get-file-config)]
       (if-not ( nil? file-conf)
         (reset! config file-conf)))))


;;file utility functions
(defn- list-subdirs
  "creates list of sub-directory names matching pattern in the given directory"
  [parent-dir pattern]
  (->> (file-seq (io/file parent-dir))
       (filter #(.isDirectory %))
       (map #(.getName %))
       (filter #(not (nil? (re-seq  pattern %))))))

(defn- dbdir-to-key [pattern dir]
  (keyword (second (clojure.string/split dir pattern))))

;;START LvlDB specific stuff
;; from config
;; (note: this is lvlDB specific, but no attempt is (yet) made to do
;; anything with the :db-type piece of config - we always open a lvlDB
(defn ^{:private true} store-dir [] (@config :db-dir))
(defn ^{:private true} snap-db-pattern [] (read-string (@config :minor-db-pattern)))
(defn ^{:private true} current-db [] (@config :current-db))
(defn ^{:private true} current-db-options [] (eval (@config :current-db-options)))
(defn ^{:private true} snap-db-options [] (eval (@config :minor-db-options)))

;;Snaps can be known by three different names used in different contexts:
;;   - the 'tag', e.g. "test1"
;;   - the 'snap-name': db/ store name (directory name in disk with
;;     levelDB, e.g. "snap-test1"
;;   - the snap-key, e.g. :test1, used to retrieve the db (class
;;     implementing KVStore protocol) from the 'snaps' atom
;; To eliminate confusion, we adopt a convention of always passing the
;; tag between the functions in this file and then converted to the
;; required form within the function.
(defn tag->snap-name [tag] (str (snap-db-pattern) tag))
(defn tag->snap-key [tag] (keyword tag))

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
  "closes single lvlDB. performs check to see if already closed"
  [db]
  (if (open? db)
    (kv/close db)
    db))

;;END LvlDB specific stuff

;;*********************OPEN/ CLOSE DB FUNCTIONS***********************
;;************General open/close current & snap functions*************

(defn- snap-map []
  (let [conns (list-subdirs (store-dir) (snap-db-pattern))]
    (zipmap
     (map #(dbdir-to-key (snap-db-pattern) %) conns)
     (map
      #(vector (str (store-dir) "/" %) (snap-db-options))
      conns))))

(defn- create-snap-db- [tag]
  (if-not ((tag->snap-key tag) @snaps)
    (kv/open-lvldb (str (store-dir) "/" (tag->snap-name tag)) (snap-db-options))
    (throw (RuntimeException. "snap already exists"))))


;****************open/ close (State-setting) functions****************

(defn- open-current-db
  "opens the current db if not already open"
  []
  (reset! curr-db
    (if (nil? (:open? @curr-db))
      (open-db- (str (store-dir) "/" (current-db)) (current-db-options))
      (open-db- @curr-db))))

(defn- close-current-db
  "close the current db if not already closed"
  []
  (reset! curr-db (close-db- @curr-db)))

(defn- open-snap
  "opens snap with given tag"
  [tag]
  (let [k (tag->snap-key tag)
        v (k @snaps)]
    (swap! snaps assoc k (open-db- v))))

(defn- open-snaps
  "open all snap dbs in config store dir"
  []
  (map
   (fn [[k v]] (swap! snaps assoc k (open-db- v)))
   (snap-map)))

(defn- close-snaps
  "close all snaps"
  []
  (map
   (fn [[k v]] (swap! snaps assoc k (close-db- v)))
   (snap-map)))

(defn- create-snap
  "create snap with supplied name
  converts name to keyword and adds snap prefix"
  [tag]
  (swap! snaps assoc (tag->snap-key tag) (create-snap-db- tag)))

;*********************************************************************
;***************************Clojure api*******************************
;********************private api helper fns***************************

(defn- ->snap-db
  [snap-db]
  (map
   #(let [[k v] %]
      (kv/put snap-db k v))
   (with-open [snap (kv/snapshot @curr-db)]
     (kv/iterator snap))))

(defn- seq->chan
  [coll f chan]
  (async/thread
   (do
     (doseq [i (map f coll)]
       (async/>!! chan i))
     (async/close! chan))))


;**********************public clojure api fns*************************
;*******************startup/ shutdown functions***********************
(defn startup
  ([]
     (set-config!)
     (try
       (open-snaps)
       (close-snaps)
       (open-current-db)
       (catch Exception e
         (println (str "caught exception: " (.getCause e)
                       (.getMessage e))))))
  ([conf]
     (set-config! conf)
     (startup)))

(defn shutdown []
  (try
    (close-snaps)
    (close-current-db)
    (catch Exception e
      (str "caught exception: " (.getCause e)
           (.getMessage e)))))

;*************************other functions*****************************
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
  ([f]
     (map f
          (with-open [snap (kv/snapshot @curr-db)]
            (kv/iterator snap))))
  ([] (get-current identity)))

(defn get-current-async
  "delivers the current state into supplied core-async channel"
  ([f chan]
     (seq->chan       
      (with-open [snap (kv/snapshot @curr-db)]
        (kv/iterator snap))
      f chan))
  ([channel] (get-current-async identity channel)))

(defn snap-to
  "copy the (snap-shotted) state of the current db into a new named snap"
  [tag]
  (do
    (create-snap tag)
    (let [db ((tag->snap-key tag) @snaps)]
      (->snap-db db))))

(defn get-snap
  "maps the supplied fn over lazy-seq representing the snap
   results in a new lazy-seq with elements of form [k v]"
  ([tag f]
     (let [t-key (tag->snap-key tag)]
       (if-not (t-key @snaps)
         (throw (RuntimeException. "Snap does not exist!"))
         (do
           (open-snap tag)
           (map f (kv/iterator ((tag->snap-key tag) @snaps)))))))
  ([tag] (get-snap tag identity)))

(defn get-snap-async
  "delivers the named snap into supplied core.async channel"
  ([tag f chan]
     (let [t-key (tag->snap-key tag)]
       (if-not (t-key @snaps)
         (throw (RuntimeException. "Snap does not exist!"))
         (do
           (open-snap (tag))
           (seq->chan
            (kv/iterator ((tag->snap-key) @snaps))
            f chan)))))
  ([tag chan] (get-snap-async tag identity chan)))


;; Query state functions
(defn is-alive? []
  (if (:open? @curr-db) true false))

(defn list-snaps []
  (map name (keys @snaps)))

(defn ?current[]
  (deref curr-db))

(defn ?config []
  (deref config))

(defn- list-snaps-status []
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

;;Usage examples for the async calls
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
