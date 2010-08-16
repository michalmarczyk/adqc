(ns adqc.scratchpad)

(comment

  "the LQP story, first take; moved here for now to get a clean state
  in adqc.lqp"

;;; TODO: use IllegalStateException for ambiguous column names etc.

;;; catalog should be queried for schema information, cardinalities
;;; (if available...?) and the like, connection-info is for whatever
;;; is needed to reach the given data source
(defprotocol PPhysicalSource
  (get-relation-list [self])
  (get-relation-head [self rel]))

(defrecord PhysicalSource [node-catalog connection-info])

;;; every operator returning a set of tuples will need to implement this
(defprotocol PRelation
  (get-head [self] "Returns a seqable of column names.")
  (has-column [self col] "Returns self when col is recognised, nil otherwise."))

(defrecord PhysicalRelation
  [name physical-source]
  PRelation
  (get-head [self] (get-relation-head physical-source name))
  (has-column [self col]
    (let [h (get-head self)]
      (if (some #{col} h) self))))

;;; TODO: should I throw 'ambiguity exception' here or at the call site?
(defn find-column-source [col srcs]
  (let [candidates (filter #(has-column % col) srcs)]
    (case (count candidates)
      0 (throw (IllegalStateException. (str "Could not locate column: " col)))
      1 (first candidates)
      (throw (IllegalStateException. (str "Ambiguous column name: " col))))))

#_
(defn )

#_
(defn preprocess-query
  "Takes a catalogue and a freshly parsed query AST and fills in
  the blanks (attribute sources &c.)."
  [catalog query]
  (let [sl (:select-list query)
        fl (:from-list query)]
    ;; how about grouping a bunch of assertions here
    (if (some is-star? sl) ; what about COUNT(*) etc.
      (if (< 1 (count sl))
        (throw (IllegalStateException. "Malformed select list"))
        (let [h (mapcat get-head fl)]
          (if (< (count (set h)) (count h))
            (throw (IllegalStateException. "Ambiguous column names in the head"))
            (map make-column )))) ; <- TODO: workin' here!
      ;; TODO: take into account AS annotations on cols and rels,
      ;; look into expressions
      ())))

#_
(defn initial-lqp [catalog query]
  (let [query (preprocess-query catalog query)]
    ...))

)

(comment
  (= (to-sql (-> "f(foo.bar)"
                 antlr-parse-expression
                 antlr->clojure
                 transform-ast))
     "f(foo.bar)"))

#_
(defn parse-sql
  "Parses the given SQL with OGSA-DAI's ANTLR-based parser
  and converts the result to a Clojure tree."
  [sql]
  )

;;; should desperation set in
#_
(defrecord AmbiguousStarExpression [children]
  ToSQL
  (to-sql
   [self]
   (throw (IllegalArgumentException.
           "ambiguous star cannot be represented in SQL")))
  SQLExpression
  (attributes [self] (apply set/union (map attributes children)))
  (rename-attributes
   [self m]
   (AmbiguousStarExpression. (map #(rename-attributes % m) children))))

#_
(let [sql-query-parser (SQLQueryParser/getInstance)]
  
  (defn antlr-parse-sql [sql]
    (.parseSQL sql-query-parser sql))
  
  (defn antlr-parse-condition [sql]
    (.parseSQLForCondition sql-query-parser sql))
  (defn antlr-parse-derived-column [sql]
    (.parseSQLForDerivedColumn sql-query-parser sql))
  (defn antlr-parse-literal [sql]
    (.parseSQLForLiteral sql-query-parser sql))
  (defn antlr-parse-value-expression-or-star [sql]
    (.parseSQLForValueExpressionOrStar sql-query-parser sql))
  (defalias antlr-parse-expression antlr-parse-value-expression-or-star)
  
  (defn antlr-generate-sql [ast]
    (.generateSQL sql-query-parser ast))

  (defn antlr-generate-condition [ast]
    (.generateSQLForCondition sql-query-parser ast))
  (defn antlr-generate-query [ast]
    (.generateSQLForQuery sql-query-parser ast))
  (defn antlr-generate-value-expression-or-star [ast]
    (.generateSQLForValueExpressionOrStar sql-query-parser ast)))

#_
(defmethod transform-node ::antlr-function
  [{[f & args] :children}]
  (FunctionApplicationExpression. f args))

#_
(defmethod transform-node ::antlr-tablecolumn
  [{children :children}]
  (if (next children)
    (Attribute. (second children) (first children) nil)
    (Attribute. (first children) nil nil)))

#_
(defmethod transform-node ::antlr-infix-plus
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "+") x y))

#_
(defmethod transform-node ::antlr-infix-minus
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "-") x y))

#_
(defmethod transform-node ::antlr-infix-div
  [{[x y] :children}]
  (InfixOperatorExpression. (InfixOperator. "/") x y))

;;; I could add an extra "context" argument to the transform-node
;;; multimethod; it could be used to disambiguate stars etc.

;;; original defrecord forms...

#_
(defrecord InfixOperatorExpression [op lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs op rhs]))))
  SQLExpression
  (attributes
   [self]
   (set/union (attributes lhs)
              (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixOperatorExpression.
    op
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

#_
(defrecord FunctionApplicationExpression [fn args]
  ToSQL
  (to-sql
   [self]
   (str fn "(" (apply str (interpose ", " (map to-sql args))) ")"))
  SQLExpression
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes
   [self m]
   (vec (map (partial rename-attributes m) args))))

#_
(defrecord Attribute [id src t]
  ToSQL
  (to-sql
   [self]
   (if src (str src "." id) id))
  SQLExpression
  (attributes [self] #{self})
  (rename-attributes
   [self m]
   (if-let [new-id (m id)]
     (Attribute. new-id src t)
     self)))

#_
(defrecord ColumnStar []
  ToSQL
  (to-sql [self] "*")
  SQLExpression
  (attributes [self] #{})
  (rename-attributes [self _] self))

#_
(defrecord InfixPredicateExpression [pred lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs pred rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixPredicateExpression.
    pred
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

#_
(defrecord PredicateConjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "AND" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateConjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

#_
(defrecord PredicateDisjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "OR" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateDisjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

#_
(defrecord PredicateNegationExpression [arg]
  ToSQL
  (to-sql [self] (str "NOT " (parenthesise arg)))
  SQLExpression
  (attributes [self] (attributes arg))
  (rename-attributes
   [self m]
   (PredicateNegationExpression. (rename-attributes arg m))))

#_
(defrecord IsNullExpression [col]
  ToSQL
  (to-sql [self] (str (to-sql col) " IS NULL"))
  SQLExpression
  (attributes [self] (attributes col))
  (rename-attributes
   [self m]
   (IsNullExpression. (rename-attributes col m))))

#_
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

;;; from defoperator

#_
(alter-var-root #'operator-qualified-names
                conj
                (symbol (str (.name *ns*)
                             "."
                             '~java-name)))

#_
(def operator-qualified-names #{})

#_
(defmacro import-all-operators []
  `(do ~@(map (fn [op-name] `(import ~op-name))
              operator-qualified-names)))
