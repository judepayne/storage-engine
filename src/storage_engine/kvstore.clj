(ns storage-engine.kvstore
  (:refer-clojure :exclude [get])
  (:require [clj-leveldb :as l]
            [byte-streams :as bs]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io
            File
            Closeable]
            [java.util
             UUID]))

;;****************************************************************************
;;                      WRAPPERS FOR VARIOUS K,V Stores                     ;;
;;                            Implemented so far:                           ;;
;;  1. Leveldb e.g. (open-db :lbd "my-db" options)                          ;;
;;****************************************************************************

;;Protocol for get/ put mechanics that all implementations should follow
(defprotocol KVStore
  (put [this k v])
  (get [this k])
  (update [this f k])
  (delete [this k])
  (bounds [this])
  (iterator [this] [this start end])
  (snapshot [this]))

;;Protocol for DB management
(defprotocol KVStoreMgt
  (compact [this])
  (close [this])
  (stats [this])
  (destroy [db-name]))

;;Protocol for enumerating available keyspaces (e.g. stores)
(defprotocol KVStoreEnum
  (enumerate-keyspaces [_ dir pattern]))


;;****************************************************************************
;;                                LEVELDB Section                           ;;
;;                        Implemnts DB Protocol for leveldb                 ;;
;;****************************************************************************

(defrecord lvlDB [db open? conn options])
(defrecord lvlSnapshot [snap]
  Closeable
  (close [this] (.close (:snap this)))
  (finalize [this] (close this)))

;;open/ create db factory function - returns lvlDB record
(defn- open-single-lvldb [conn options]
  (let [db (l/create-db
              (doto (File. conn) .deleteOnExit)
                options)]
    (-> (lvlDB. db true conn options)
        (with-meta {:created (System/currentTimeMillis)}))))

(defn- list-dbs [dir pattern]
  (->> (file-seq (io/file dir))
       (filter #(.isDirectory %))
       (map #(.getName %))
       (filter #(not (nil? (re-seq pattern %))))))

(defn- dir-to-name [pattern dir]
  (second (clojure.string/split dir pattern)))

(defn open-lvldb
  ([conn options]
    (open-single-lvldb conn options))
  ([dir pattern opts]
    (let [conns (list-dbs dir pattern)]
      (zipmap
       (map #(keyword (dir-to-name pattern %)) conns)
       (map
          #(open-lvldb (str dir "/" %) opts)
          conns)))))

(extend-type lvlSnapshot
  KVStore
  (get [this k] (l/get (:snap this) k))
  (iterator [this] (l/iterator (:snap this))))

(extend-type lvlDB
  KVStore
  (put [this k v] (l/put (:db this) k v))
  (get [this k] (l/get (:db this) k))
  (update [this f k]  ;not atomic - relies on single threaded writer
    (let [v (l/get (:db this) k)]
      (l/put (:db this) k (f v))))
  (delete [this k] (l/delete (:db this) k))
  (bounds [this] (l/bounds (:db this)))
  (iterator ([this] (l/iterator (:db this)))
            ([this start end] (l/iterator (:db this) start end)))
  (snapshot ([this] (lvlSnapshot. (l/snapshot (:db this))))))

(extend-type lvlDB
  KVStoreMgt
  (compact [this] (l/compact (:db this)))
  (close [this] (.close (:db this))
                (-> (assoc this :open? false)
                    (with-meta (assoc (meta this)
                                 :closed (System/currentTimeMillis))))) ;;returns new lvlDB instance
  (stats [this] (l/stats (:db this) ""))
  (destroy [db-name] (l/destroy-db (str db-name)))) ;; only works on a closed db


(extend-type lvlDB
  KVStoreEnum
  (enumerate-keyspaces [_ dir pattern]
     (->> (file-seq (io/file dir))
       (filter #(.isDirectory %))
       (map #(.getName %))
       (filter #(not (nil? (re-seq pattern %)))))))


;;USEFUL: Example Options for leveldb
(def ldb-options {  :key-encoder name
                    :key-decoder (comp keyword bs/to-string)
                    :val-decoder (comp edn/read-string bs/to-char-sequence) ;;needs not be set!
                    :val-encoder pr-str
                    :cache-size  (* 32 1024 1024)
                    })

;;****************************************************************************
;;                                ANOTHERDB Section                         ;;
;;                     Implemnts DB Protocol for another db                 ;;
;;****************************************************************************

;;Second k,v store not yet implemented




;;****************************************************************************
;;                                 Example usage                            ;;
;;                       Example of opening a db + usage                    ;;
;;****************************************************************************
;;Use an implementation specific function to open a db/ KVStore..
;; Open a leveldb database called 'my-ldb' in current directory:
(comment

  (def dir "store/current")
  (def db (open-lvldb dir ldb-options))

  ;;..After that extract information about the db from the Record..
  ;; Get information about the db
  db
  (:conn db)
  (:db db)
  (-> (:options db)
      :cache-size)
  (meta db)

  ;;..And call functions of the KVStore protocol to access common functionality...
  ;; Do KV Store type stuff with the db
  (put db :k 3)
  (put db :j 4)
  (put db :l "text as well!")
  (get db :k)
  (delete db :k)
  (bounds db)

  ;;snapshot stuff
  (with-open [snap (snapshot db)]
    (get snap :k))


  ;;iterator is a lazy-seq
  (take 3 (iterator db))
  (map #(let [[k v] %] k) (take 50 (iterator db)))
  (map #(let [[k v] %] k) (take 5 (iterator db :key--10 :key--20)))
  ;; Do DB Management type stuff with the db
  (compact db)
  (stats db)
  (def db-closed (close db)) ;;new lvlDB instance returned. cannot be re-opened
  db-closed
  (meta db-closed)
  (destroy db))

(defn put-batch
  [db_ keyvalz]
  (loop [left keyvalz
         n 0]
    (if (empty? left) (* n 2)
        (let [h (first left)
              r (rest left)]
          (put db_ (first h) (second h))
          (recur r (inc n))))))

(def text "Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world. It is a way I have of driving off the spleen and regulating the circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing up the rear of every funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires a strong moral principle to prevent me from deliberately stepping into the street, and methodically knocking people's hats off—then, I account it high time to get to sea as soon as I can. This is my substitute for pistol and ball. With a philosophical flourish Cato throws himself upon his sword; I quietly take to the ship. There is nothing surprising in this. If they but knew it, almost al")
  ;; End of example usage


