(ns adqc.sql.records
  (:use adqc.sql.protocols
        [adqc.util :only [to-java-name to-factory-name]]
        [clojure
         [string :as str :only []]
         [set :as set :only []]
         [walk :as walk :only []]]))

(defmacro defexpression [clj-name fields & method-impls]
  (let [java-name    (to-java-name clj-name)
        factory-name (to-factory-name clj-name)
        impls-map (zipmap (map (comp keyword first) method-impls)
                          (for [[m-name & m-tail] method-impls]
                            (let [transformed-tail
                                  (walk/prewalk
                                   (fn [f]
                                     (if (and (seq? f)
                                              (seq f)
                                              (= (first f) factory-name))
                                       (list* 'new java-name (rest f))
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
           (new ~java-name ~@fields)))))

;;; TODO: use clojure.lang.Named where it makes sense!
;;; (although it includes getNamespace...)

;;; TODO: maybe reserve the word 'operator' for LQP operators
(defrecord InfixOperator [op-name]
  ToSQL
  (to-sql [self] op-name))

(defn make-infix-operator [op-name]
  (InfixOperator. op-name))

(defexpression infix-operator-expression [op lhs rhs]
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs op rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (make-infix-operator-expression op
                                    (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

(defexpression function-application-expression [fn-name args]
  (to-sql [self]
    (str fn-name "(" (apply str (interpose ", " (map to-sql args))) ")"))
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes [self m]
    (make-function-application-expression
     fn-name
     (vec (map (partial rename-attributes m) args)))))

;;; does this present a case for decoupling factory names and record names,
;;; or is this not an issue, say, because of namespacing?
(defexpression attribute [id src attr-type]
  (to-sql [self] (if src (str src "." id) id))
  (attributes [self] #{self})
  (rename-attributes [self m]
    (if-let [new-id (m id)]
      (make-attribute new-id src attr-type)
      self)))

(defexpression column-star []
  (to-sql [self] "*")
  (attributes [self] #{})
  (rename-attributes [self _] self))

(defrecord InfixPredicate [pred-name]
  ToSQL
  (to-sql [self] pred-name))

(defn make-infix-predicate [pred-name]
  (InfixPredicate. pred-name))

(defexpression infix-predicate-expression [pred lhs rhs]
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs pred rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (make-infix-predicate-expression pred
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
    (make-and-expression (rename-attributes lhs m)
                         (rename-attributes rhs m))))

;;; FIXME: this ought to happen inside defexpression
;(derive PredicateConjunctionExpression ::compound-predicate)
(derive AndExpression ::compound-predicate)

(defexpression or-expression [lhs rhs]
  (to-sql [self] (apply str (interpose " " (map parenthesize [lhs "OR" rhs]))))
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes [self m]
    (make-or-expression (rename-attributes lhs m)
                        (rename-attributes rhs m))))

;;; FIXME: as above
;(derive PredicateDisjunctionExpression ::compound-predicate)
(derive OrExpression ::compound-predicate)

(defexpression negation-expression [pred]
  (to-sql [self] (str "NOT " (parenthesize pred)))
  (attributes [self] (attributes pred))
  (rename-attributes [self m]
    (make-negation-expression (rename-attributes pred m))))

;;; FIXME: as above
;(derive PredicateNegationExpression ::compound-predicate)
(derive NegationExpression ::compound-predicate)

(defexpression is-null-expression [col]
  (to-sql [self] (str (to-sql col) " IS NULL"))
  (attributes [self] (attributes col))
  (rename-attributes [self m]
    (make-is-null-expression (rename-attributes col m))))

;;; TODO:
;;; What should rename-attributes on a column with AS actually do?
(defexpression column-expression [attr as]
  (to-sql [self] (str (to-sql attr) (when as (str " AS " as))))
  (attributes [self] #{attr})
  (rename-attributes [self m]
    (make-column-expression (rename-attributes attr m) as)))

(defn is-star? [col-expr]
  (instance? ColumnStar (:attr col-expr)))

(defexpression select-list [cols]
  (to-sql [self]
    (apply str (cons "SELECT " (interpose " " (map to-sql cols)))))
  (attributes [self] (apply set/union (map attributes cols)))
  (rename-attributes [self m]
    (make-select-list (map #(rename-attributes % m) cols))))

(defexpression from-list [sources]
  (to-sql [self]
    (apply str (cons "FROM " (interpose " " (map to-sql sources)))))
  (attributes [self]
    (apply set/union (map attributes sources)))
  (rename-attributes [self m]
    (make-from-list (map #(rename-attributes % m) sources))))

;;; TODO:
;;; Hopefully this token type is not being reused for multiple purposes...
(defexpression relation-expression [id as]
  (to-sql [self]
    (str id (when as (str " AS " as))))
  (attributes [self] #{})
  (rename-attributes [self _] self))

(defexpression where-clause [pred]
  (to-sql [self] (str "WHERE " (to-sql pred)))
  (attributes [self] (attributes pred))
  (rename-attributes [self m] (make-where-clause (rename-attributes pred m))))

;;; TODO: add fields for GROUP BY & HAVING
(defexpression sql-query [select-list from-list where-clause]
  (to-sql [self]
    (apply str (interpose " " (map to-sql [select-list from-list where-clause]))))
  (attributes [self]
    (apply set/union (map attributes [select-list from-list where-clause])))
  (rename-attributes [self m]
    (let [f (fn rename-attributes-helper [item] (rename-attributes item m))]
      (make-sql-query (f select-list) (f from-list) (f where-clause)))))

;;; TODO: add field for ORDER BY
(defexpression sql-statement [query]
  (to-sql [self] (to-sql query))
  (attributes [self] (attributes query))
  (rename-attributes [self m] (make-sql-statement (rename-attributes query m))))
