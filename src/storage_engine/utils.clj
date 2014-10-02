(ns storage-engine.utils)

(defn map-vals
  "map f over the vals in a map"
  [f coll]
  (into {} (for [[k v] coll] [k (f v)])))

(defn filter-vals
  "filter on the vals in a map"
  [pred coll]
  (into {} (filter #(pred (val %)) coll)))

(defn converge-to
  "unifies/converts the vals of the supplied maps to a common, desired state.
   The first map is dominant. Other maps are secondary and may contain additional items.
   The first map is filtered by the pred(icate) applied over its vals and has function f applied over its vals.
   This is merged with items in the other maps (but not in the first) with f mapped over their vals."
  [f pred & maps]
  (when (some identity maps)
    (let [front-map (filter-vals pred (first maps))
          maps2 (cons front-map (rest maps))
          maps3 (reduce #(merge %2 %1) maps2)]
      (map-vals f maps3))))
