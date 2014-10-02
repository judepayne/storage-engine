;; DEPRECATED!! ;;
;; DEPRECATED!! ;;
;; DEPRECATED!! ;;
;; DEPRECATED!! ;;

(ns storage-engine.lvldbwrapper
  (:refer-clojure :exclude [get sync])
  (:require
   [clojure.java.io :as io]
   [byte-streams :as bs]
   [clojure.edn :as edn])
  (:import [org.iq80.leveldb
            Options
            DB
            CompressionType
            DBIterator])
  (:import org.fusesource.leveldbjni.JniDBFactory))


;; HawtJNI tends to leave trash in /tmp, repeated loading of this
;; namespace can add up.  We allow the 10 newest files to stick
;; around in case there's some sort of contention, since we're only
;; trying to put an upper bound on how many of these files stick around.
(let [tmp-dir (str
                (System/getProperty "java.io.tmpdir")
                (System/getProperty "file.separator")
                "com.factual.clj-leveldb")]
  (let [d (io/file tmp-dir)]
    (doseq [f (->> (.listFiles d)
                (sort-by #(.lastModified %))
                reverse
                (drop 10))]
      (.delete f)))
  (System/setProperty "library.leveldbjni.path" tmp-dir))

;;****************************************************************************
;;                                Opening level db                          ;;
;;****************************************************************************

(def ^:private default-options
  {:key-decoder name
   :key-encoder (comp keyword bs/to-string)
   :val-decoder (comp edn/read-string bs/to-char-sequence) ;;needs not be set!
   :val-encoder (comp bs/to-byte-array pr-str)
   :compress? true
   :cache-size (* 32 1024 1024)
   :block-size (* 16 1024)
   :write-buffer-size (* 32 1024 1024)
   :create-if-missing? true
   :error-if-exists? false})

(def ^:private option-setters
  {:create-if-missing?     #(.createIfMissing ^Options %1 %2)
   :error-if-exists?       #(.errorIfExists ^Options %1 %2)
   :write-buffer-size      #(.writeBufferSize ^Options %1 %2)
   :block-size             #(.blockSize ^Options %1 %2)
   :block-restart-interval #(.blockRestartInterval ^Options %1 %2)
   :max-open-files         #(.maxOpenFiles ^Options %1 %2)
   :cache-size             #(.cacheSize ^Options %1 %2)
   :comparator             #(.comparator ^Options %1 %2)
   :paranoid-checks?       #(.paranoidChecks ^Options %1 %2)
   :compress?              #(.compressionType ^Options %1
                              (if %2 CompressionType/SNAPPY CompressionType/NONE))
   :logger                 #(.logger ^Options %1 %2)})

(defn- fill-option-defaults
  "set missing options"
  [opts]
  (reduce
   (fn [m [k v]] (assoc m k v))
   default-options
   opts))

(defn- convert-options
  "given a map of options e.g. {:cache-size (*24 1024 1024) etc}
  convert to java options object to be passed to level db jni"
  [options]
  (let [opts (Options.)]
    (doseq [[k v] options]
       (when (and v (contains? option-setters k))
          ((option-setters k) opts v)))
     opts))


(defn open
  ([path] (open path {}))
  ([path options]
  (let [opts-map (fill-option-defaults options)
        opts (convert-options opts-map)
        dbfile (io/file path)
        factory (JniDBFactory.)]
    [(.open factory dbfile opts) opts-map])))



;;****************************************************************************
;;                                Basic operations                          ;;
;;****************************************************************************

(defn put
  [db key value]
  (.put db key value))

(defn get
  [db key]
  (.get db key))

(defn delete
  [db key]
  (.delete db key))

;;****************************************************************************
;;                                Batch operations                          ;;
;;****************************************************************************

(defn create-write-batch
  "returns a WriteBatch object"
  [db]
  (.createWriteBatch db))

(defn batch-put
  [batch key value]
  (.put batch key value))

(defn batch-delete
  [batch key]
  (.delete batch key))

(defn write-batch
  [db batch]
  (.write db batch))

(comment
  (defrecord LevelIterator [inner]
    java.io.Closeable
    Iterator
    (seek! [this value] (.seek inner (to-db value)))
    (as-seq [this]
            (->> (iterator-seq inner) (map expand-iterator-str)))
    (close [this] (.close inner))))


;;****************************************************************************
;;                                DB Mgt operations                         ;;
;;****************************************************************************

(defn stats
  [db]
  (.stats db))

(defn compact
  [db]
  (.compact db))
