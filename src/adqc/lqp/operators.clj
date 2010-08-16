(ns adqc.lqp.operators
  (:use [adqc.util :only [to-java-name to-factory-name]]))

(defmacro defoperator [clj-name]
  (let [java-name    (to-java-name clj-name :suffix 'operator)
        factory-name (to-factory-name clj-name :suffix 'operator)]
    `(do (defrecord ~java-name [~'head ~'body ~'children])
         (defn ~factory-name [~'head ~'body ~'children]
           (new ~java-name ~'head ~'body ~'children))
         (defmethod print-method ~java-name [op# ~' ^java.io.Writer w]
           (.write ~'w "#:")
           (.write ~'w ~(clojure.core/name java-name))
           (print-method (select-keys op# [:head :children]) ~'w)))))

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

(defn operator-qualified-java-name [clj-name]
  (symbol "adqc.lqp.operators"
          (name (to-java-name clj-name :suffix 'operator))))

(defmacro extend-operator [clj-name & specs]
  `(extend-type ~(operator-qualified-java-name clj-name)
     ~@specs))

;;; a more general macro would be "extend-protocol-converting-names" (?)
;;; which could be used as
;;; (extend-protocol-converting-names p #(to-java-name % :suffix 'operator) ...)
;;; -- presumably, that would go in adqc.util
(defmacro extend-protocol-to-operators [p & specs]
  `(extend-protocol ~p
     ~@(map (fn [spec]
              (if (symbol? spec)
                ~(operator-qualified-java-name spec)
                spec))
            specs)))
