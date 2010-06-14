(ns adqc.sql.records
  (:use adqc.sql.protocols
        [clojure
         [string :as str :only []]
         [set :as set :only []]
         [walk :as walk :only []]]))

(defmacro defexpression [factory-name fields & method-impls]
  (let [java-name (->> factory-name
                       name
                       (.split #"-")
                       (mapcat (juxt #(Character/toUpperCase
                                       (.charAt ^String % 0))
                                     #(-> ^String %
                                          (.substring 1)
                                          (->> (apply str)))))
                       (apply str)
                       symbol)
        java-name-dot (symbol (str java-name "."))
        impls-map (zipmap (map (comp keyword first) method-impls)
                          (for [[m-name & m-tail] method-impls]
                            (let [transformed-tail
                                  (walk/prewalk
                                   (fn [f]
                                     (if (and (seq? f)
                                              (seq f)
                                              (= (first f) factory-name))
                                       (cons java-name-dot (rest f))
                                       f))
                                   m-tail)]
                              `(~m-name ~@transformed-tail))))]
    `(do (defrecord ~java-name ~fields
           ~'ToSQL
           ~(:to-sql impls-map)
           ~'SQLExpression
           ~(:attributes impls-map)
           ~(:rename-attributes impls-map))
         (defn ~factory-name ~fields
           (~java-name-dot ~@fields)))))

(defrecord InfixOperator [op-name]
  ToSQL
  (to-sql [self] op-name))

(defn infix-operator [op-name]
  (InfixOperator. op-name))

(defexpression infix-operator-expression [op lhs rhs]
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs op rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (infix-operator-expression op
                               (rename-attributes lhs m)
                               (rename-attributes rhs m))))

(defexpression function-application-expression [fn-name args]
  (to-sql [self]
    (str fn-name "(" (apply str (interpose ", " (map to-sql args))) ")"))
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes [self m]
    (function-application-expression
     fn-name
     (vec (map (partial rename-attributes m) args)))))

;;; does this present a case for decoupling factory names and record names,
;;; or is this not an issue, say, because of namespacing?
(defexpression attribute [id src attr-type]
  (to-sql [self] (if src (str src "." id) id))
  (attributes [self] #{self})
  (rename-attributes [self m]
    (if-let [new-id (m id)]
      (attribute new-id src attr-type)
      self)))

(defexpression column-star []
  (to-sql [self] "*")
  (attributes [self] #{})
  (rename-attributes [self _] self))

(defrecord InfixPredicate [pred-name]
  ToSQL
  (to-sql [self] pred-name))

(defn infix-predicate [pred-name]
  (InfixPredicate. pred-name))

(defexpression infix-predicate-expression [pred lhs rhs]
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs pred rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (infix-predicate-expression pred
                                (rename-attributes lhs m)
                                (rename-attributes rhs m))))

;;; FIXME: switch over to a custom hierarchy?
(defmulti parenthesize type)

(defmethod parenthesize ::compound-predicate
  [expr] (str "(" (to-sql expr) ")"))
(defmethod parenthesize :default
  [expr] (to-sql expr))

(defexpression and-expression [lhs rhs]
  (to-sql [self] (apply str (interpose " " (map parenthesize [lhs "AND" rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (and-expression (rename-attributes lhs m)
                    (rename-attributes rhs m))))

;;; FIXME: this ought to happen inside defexpression
;(derive PredicateConjunctionExpression ::compound-predicate)
(derive AndExpression ::compound-predicate)

(defexpression or-expression [lhs rhs]
  (to-sql [self] (apply str (interpose " " (map parenthesize [lhs "OR" rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (or-expression (rename-attributes lhs m)
                   (rename-attributes rhs m))))

;;; FIXME: as above
;(derive PredicateDisjunctionExpression ::compound-predicate)
(derive OrExpression ::compound-predicate)

(defexpression negation-expression [pred]
  (to-sql [self] (str "NOT " (parenthesize pred)))
  (attributes [self] (attributes pred))
  (rename-attributes [self m]
    (negation-expression (rename-attributes pred m))))

;;; FIXME: as above
;(derive PredicateNegationExpression ::compound-predicate)
(derive NegationExpression ::compound-predicate)

(defexpression is-null-expression [col]
  (to-sql [self] (str (to-sql col) " IS NULL"))
  (attributes [self] (attributes col))
  (rename-attributes [self m]
    (is-null-expression (rename-attributes col m))))
