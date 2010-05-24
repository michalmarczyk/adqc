(ns adqc.util)

(defn extract-info [extractors item]
  (reduce (fn [m [k e]]
            (assoc m k (e item)))
          {}
          extractors))

(defn make-extractor [arg spec]
  (cond (symbol? spec)
        `(fn [~arg]
           (. ~arg ~spec))
        (keyword? spec)
        `(fn [~arg]
           (~(symbol (name spec)) ~arg))
        (and (seq? spec) (#{'-> '->>} (first spec)))
        `(fn [~arg]
           (~(first spec) (. ~arg ~(second spec)) ~@(nnext spec)))))

(defmacro defextractors [name hint & exts]
  (let [arg (with-meta (gensym "item") {:tag hint})]
    `(def ~name
          ~(into {} (map (fn [[k e-spec]]
                           [k (make-extractor arg e-spec)])
                         (partition 2 exts))))))
