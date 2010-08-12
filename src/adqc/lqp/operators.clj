(ns adqc.lqp.operators
  (:use [adqc.util :only [to-java-name to-factory-name]]))

#_
(def operators #{})

(defmacro defoperator [clj-name]
  (let [java-name    (to-java-name clj-name :suffix 'operator)
        factory-name (to-factory-name clj-name :suffix 'operator)]
    `(do (defrecord ~java-name [~'head ~'body ~'children])
         (defn ~factory-name [~'head ~'body ~'children]
           (new ~java-name ~'head ~'body ~'children))
         (defmethod print-method ~java-name [op# ~' ^java.io.Writer w]
           (.write ~'w "#:")
           (.write ~'w ~(clojure.core/name java-name))
           (print-method (select-keys op# [:head :children]) ~'w))
         #_
         (alter-var-root #'operators
                         conj
                         (symbol (str (.name *ns*)
                                      "."
                                      '~java-name))))))

(defmacro defoperators [& names]
  `(do ~@(map (fn [name] `(defoperator ~name)) names)))

(defoperators
  root
  table-scan
  project
  select
  product
  inner-theta-join
  exchange
  sort)
